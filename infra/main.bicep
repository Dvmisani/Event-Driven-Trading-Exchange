@description('Azure region for all exchange infrastructure resources.')
param location string = resourceGroup().location

@description('Globally unique prefix for the deployment.')
param prefix string = 'dvmisani-exchange'

@secure()
@description('Azure SQL administrator password.')
param sqlAdminPassword string

@description('Container image tag to deploy from Azure Container Registry.')
param imageTag string = 'latest'

var uniqueSuffix = uniqueString(resourceGroup().id)
var acrName = replace('${prefix}${uniqueSuffix}', '-', '')

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${prefix}-logs-${uniqueSuffix}'
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${prefix}-ai-${uniqueSuffix}'
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
  }
}

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: take(acrName, 45)
  location: location
  sku: { name: 'Basic' }
  properties: {
    adminUserEnabled: true
  }
}

resource sqlServer 'Microsoft.Sql/servers@2023-08-01-preview' = {
  name: '${prefix}-sql-${uniqueSuffix}'
  location: location
  properties: {
    administratorLogin: 'exchangeadmin'
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
    minimalTlsVersion: '1.2'
  }
}

resource sqlFirewallAzure 'Microsoft.Sql/servers/firewallRules@2023-08-01-preview' = {
  parent: sqlServer
  name: 'AllowAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource sqlDb 'Microsoft.Sql/servers/databases@2023-08-01-preview' = {
  parent: sqlServer
  name: 'TradingExchangeDb'
  location: location
  sku: {
    name: 'Basic'
    tier: 'Basic'
  }
}

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: '${prefix}-eh-${uniqueSuffix}'
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    kafkaEnabled: true
  }
}

resource eventsTopic 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubNamespace
  name: 'exchange.events'
  properties: {
    partitionCount: 4
    messageRetentionInDays: 7
  }
}

resource dlqTopic 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubNamespace
  name: 'exchange.events.deadletter'
  properties: {
    partitionCount: 2
    messageRetentionInDays: 7
  }
}

resource containerEnv 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: '${prefix}-env-${uniqueSuffix}'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

var ehAuthRuleId = resourceId('Microsoft.EventHub/namespaces/authorizationRules', eventHubNamespace.name, 'RootManageSharedAccessKey')
var ehConnectionString = listKeys(ehAuthRuleId, '2024-01-01').primaryConnectionString
var sqlConnectionString = 'Server=tcp:${sqlServer.properties.fullyQualifiedDomainName},1433;Initial Catalog=TradingExchangeDb;Persist Security Info=False;User ID=exchangeadmin;Password=${sqlAdminPassword};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;'

resource tradingApiApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${prefix}-api'
  location: location
  properties: {
    managedEnvironmentId: containerEnv.id
    configuration: {
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
      }
      registries: [
        {
          server: acr.properties.loginServer
          username: acr.listCredentials().username
          passwordSecretRef: 'acr-password'
        }
      ]
      secrets: [
        { name: 'acr-password', value: acr.listCredentials().passwords[0].value }
        { name: 'sql-conn', value: sqlConnectionString }
        { name: 'eh-conn', value: ehConnectionString }
      ]
    }
    template: {
      containers: [
        {
          name: 'trading-exchange-api'
          image: '${acr.properties.loginServer}/trading-exchange-api:${imageTag}'
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: [
            { name: 'ConnectionStrings__DefaultConnection', secretRef: 'sql-conn' }
            { name: 'Kafka__BootstrapServers', value: '${eventHubNamespace.name}.servicebus.windows.net:9093' }
            { name: 'Kafka__SecurityProtocol', value: 'SaslSsl' }
            { name: 'Kafka__SaslMechanism', value: 'Plain' }
            { name: 'Kafka__SaslUsername', value: '$ConnectionString' }
            { name: 'Kafka__SaslPassword', secretRef: 'eh-conn' }
            { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsights.properties.ConnectionString }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 3
      }
    }
  }
}

output containerAppUrl string = 'https://${tradingApiApp.properties.configuration.ingress.fqdn}'
output acrLoginServer string = acr.properties.loginServer
output eventHubsKafkaBroker string = '${eventHubNamespace.name}.servicebus.windows.net:9093'
