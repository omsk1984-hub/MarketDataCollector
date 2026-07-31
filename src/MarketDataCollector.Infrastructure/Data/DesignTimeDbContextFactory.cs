using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;

namespace MarketDataCollector.Infrastructure.Data
{
    /// <summary>
    /// Design-time фабрика для EF Core CLI (dotnet ef migrations).
    /// Позволяет генерировать миграции без запуска Worker.
    /// Строка подключения читается из env-переменных в порядке приоритета:
    /// 1. ConnectionStrings__MarketDataDb  (стандартный .NET env-провайдер)
    /// 2. MarketDataDb__Default
    /// 3. Fallback локальной строки для разработки.
    /// </summary>
    public class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<MarketDataDbContext>
    {
        private const string DefaultConnectionString =
            "Host=localhost;Port=5432;Database=MarketDataDb;Username=marketdata_user;Password=StrongPassword123!;sslmode=Disable;Include Error Detail=true";

        public MarketDataDbContext CreateDbContext(string[] args)
        {
            var connectionString = GetConnectionString();

            var optionsBuilder = new DbContextOptionsBuilder<MarketDataDbContext>();
            optionsBuilder.UseNpgsql(connectionString);

            return new MarketDataDbContext(optionsBuilder.Options);
        }

        private static string GetConnectionString()
        {
            var env = System.Environment.GetEnvironmentVariable("ConnectionStrings__MarketDataDb");
            if (!string.IsNullOrWhiteSpace(env))
                return env;

            env = System.Environment.GetEnvironmentVariable("MarketDataDb__Default");
            if (!string.IsNullOrWhiteSpace(env))
                return env;

            return DefaultConnectionString;
        }
    }
}
