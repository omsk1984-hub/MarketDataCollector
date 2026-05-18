using MarketDataCollector.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace MarketDataCollector.Infrastructure.Data
{
    public class MarketDataDbContext : DbContext
    {
        public DbSet<RawTick> RawTicks { get; set; }
        public DbSet<ConnectionLog> ConnectionLogs { get; set; }
        public DbSet<AggregatedData> AggregatedData { get; set; }

        public MarketDataDbContext(DbContextOptions<MarketDataDbContext> options) : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // RawTick configuration
            modelBuilder.Entity<RawTick>(entity =>
            {
                entity.HasKey(e => e.Id);

                // Unique constraint to prevent duplicates
                entity.HasIndex(e => new { e.Ticker, e.Exchange, e.Timestamp }).IsUnique();

                // Decimal precision: up to 18 digits, 8 after decimal point
                entity.Property(e => e.Price).HasPrecision(18, 8);
                entity.Property(e => e.Volume).HasPrecision(18, 8);
            });

            // ConnectionLog configuration
            modelBuilder.Entity<ConnectionLog>(entity =>
            {
                entity.HasKey(e => e.Id);

                entity.HasIndex(e => e.Exchange);
                entity.HasIndex(e => e.CreatedAt);
            });

            // AggregatedData configuration
            modelBuilder.Entity<AggregatedData>(entity =>
            {
                entity.HasKey(e => e.Id);

                entity.HasIndex(e => new { e.Ticker, e.Interval });
                entity.HasIndex(e => e.StartTime);

                // Decimal precision: up to 18 digits, 8 after decimal point
                entity.Property(e => e.OpenPrice).HasPrecision(18, 8);
                entity.Property(e => e.HighPrice).HasPrecision(18, 8);
                entity.Property(e => e.LowPrice).HasPrecision(18, 8);
                entity.Property(e => e.ClosePrice).HasPrecision(18, 8);
                entity.Property(e => e.Volume).HasPrecision(18, 8);
            });
        }
    }
}
