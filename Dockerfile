FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/TradingExchange.Api/TradingExchange.Api.csproj src/TradingExchange.Api/
RUN dotnet restore src/TradingExchange.Api/TradingExchange.Api.csproj
COPY src/TradingExchange.Api/ src/TradingExchange.Api/
RUN dotnet publish src/TradingExchange.Api/TradingExchange.Api.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "TradingExchange.Api.dll"]
