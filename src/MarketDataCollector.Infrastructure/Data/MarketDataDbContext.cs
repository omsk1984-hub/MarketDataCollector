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
                entity.ToTable("rawticks");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");

                entity.Property(e => e.Ticker)
                    .HasColumnName("ticker")
                    .IsRequired()
                    .HasMaxLength(20);
                entity.Property(e => e.Price)
                    .HasColumnName("price")
                    .IsRequired()
                    .HasPrecision(18, 8);
                entity.Property(e => e.Volume)
                    .HasColumnName("volume")
                    .IsRequired()
                    .HasPrecision(18, 8);
                entity.Property(e => e.Timestamp)
                    .HasColumnName("timestamp")
                    .IsRequired();
                entity.Property(e => e.Exchange)
                    .HasColumnName("exchange")
                    .IsRequired()
                    .HasMaxLength(50);
                entity.Property(e => e.ReceivedAt)
                    .HasColumnName("receivedat")
                    .IsRequired();
                entity.Property(e => e.Normalized)
                    .HasColumnName("normalized")
                    .IsRequired();

                // Unique constraint to prevent duplicates
                entity.HasIndex(e => new { e.Ticker, e.Exchange, e.Timestamp }).IsUnique();
            });

            // ConnectionLog configuration
            modelBuilder.Entity<ConnectionLog>(entity =>
            {
                entity.ToTable("connectionlogs");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");

                entity.Property(e => e.Exchange)
                    .HasColumnName("exchange")
                    .IsRequired()
                    .HasMaxLength(50);
                entity.Property(e => e.EventType)
                    .HasColumnName("eventtype")
                    .IsRequired()
                    .HasMaxLength(20);
                entity.Property(e => e.Message)
                    .HasColumnName("message")
                    .IsRequired(false);
                entity.Property(e => e.CreatedAt)
                    .HasColumnName("createdat")
                    .IsRequired();

                entity.HasIndex(e => e.Exchange);
                entity.HasIndex(e => e.CreatedAt);
            });

            // AggregatedData configuration
            modelBuilder.Entity<AggregatedData>(entity =>
            {
                entity.ToTable("aggregateddata");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");

                entity.Property(e => e.Ticker)
                    .HasColumnName("ticker")
                    .IsRequired()
                    .HasMaxLength(20);
                entity.Property(e => e.Interval)
                    .HasColumnName("interval")
                    .IsRequired()
                    .HasMaxLength(10);
                entity.Property(e => e.OpenPrice)
                    .HasColumnName("openprice")
                    .IsRequired()
                    .HasPrecision(18, 8);
                entity.Property(e => e.HighPrice)
                    .HasColumnName("highprice")
                    .IsRequired()
                    .HasPrecision(18, 8);
                entity.Property(e => e.LowPrice)
                    .HasColumnName("lowprice")
                    .IsRequired()
                    .HasPrecision(18, 8);
                entity.Property(e => e.ClosePrice)
                    .HasColumnName("closeprice")
                    .IsRequired()
                    .HasPrecision(18, 8);
                entity.Property(e => e.Volume)
                    .HasColumnName("volume")
                    .IsRequired()
                    .HasPrecision(18, 8);
                entity.Property(e => e.StartTime)
                    .HasColumnName("starttime")
                    .IsRequired();
                entity.Property(e => e.EndTime)
                    .HasColumnName("endtime")
                    .IsRequired();
                entity.Property(e => e.CreatedAt)
                    .HasColumnName("createdat")
                    .IsRequired();

                entity.HasIndex(e => new { e.Ticker, e.Interval });
                entity.HasIndex(e => e.StartTime);
            });
        }
    }
}
