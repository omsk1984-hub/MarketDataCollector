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
                entity.Property(e => e.Ticker).IsRequired().HasMaxLength(20);
                entity.Property(e => e.Price).HasColumnType("decimal(18,8)");
                entity.Property(e => e.Volume).HasColumnType("decimal(18,8)");
                entity.Property(e => e.Timestamp).IsRequired();
                entity.Property(e => e.Exchange).IsRequired().HasMaxLength(50);
                entity.Property(e => e.ReceivedAt).IsRequired();
                entity.Property(e => e.Normalized).IsRequired();

                // Unique constraint to prevent duplicates
                entity.HasIndex(e => new { e.Ticker, e.Exchange, e.Timestamp }).IsUnique();
            });

            // ConnectionLog configuration
            modelBuilder.Entity<ConnectionLog>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Exchange).IsRequired().HasMaxLength(50);
                entity.Property(e => e.EventType).IsRequired().HasMaxLength(20);
                entity.Property(e => e.Message);
                entity.Property(e => e.CreatedAt).IsRequired();

                entity.HasIndex(e => e.Exchange);
                entity.HasIndex(e => e.CreatedAt);
            });

            // AggregatedData configuration
            modelBuilder.Entity<AggregatedData>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Ticker).IsRequired().HasMaxLength(20);
                entity.Property(e => e.Interval).IsRequired().HasMaxLength(10);
                entity.Property(e => e.OpenPrice).HasColumnType("decimal(18,8)");
                entity.Property(e => e.HighPrice).HasColumnType("decimal(18,8)");
                entity.Property(e => e.LowPrice).HasColumnType("decimal(18,8)");
                entity.Property(e => e.ClosePrice).HasColumnType("decimal(18,8)");
                entity.Property(e => e.Volume).HasColumnType("decimal(18,8)");
                entity.Property(e => e.StartTime).IsRequired();
                entity.Property(e => e.EndTime).IsRequired();
                entity.Property(e => e.CreatedAt).IsRequired();

                entity.HasIndex(e => new { e.Ticker, e.Interval });
                entity.HasIndex(e => e.StartTime);
            });
        }
    }
}