using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MarketDataCollector.Infrastructure.Data.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "aggregateddata",
                columns: table => new
                {
                    id = table.Column<Guid>(type: "uuid", nullable: false),
                    ticker = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                    interval = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: false),
                    openprice = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: false),
                    highprice = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: false),
                    lowprice = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: false),
                    closeprice = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: false),
                    volume = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: false),
                    starttime = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    endtime = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    createdat = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_aggregateddata", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "connectionlogs",
                columns: table => new
                {
                    id = table.Column<Guid>(type: "uuid", nullable: false),
                    exchange = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: false),
                    eventtype = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                    message = table.Column<string>(type: "text", nullable: true),
                    createdat = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_connectionlogs", x => x.id);
                });

            // Таблица rawticks создаётся как native-партиционированная по Timestamp.
            // EF Core не умеет создавать PARTITION BY RANGE через миграции, поэтому
            // создаём через raw SQL. PRIMARY KEY включает partition key (timestamp).
            // Партиции создаются автоматически PartitionMaintenanceService'ом.
            migrationBuilder.Sql(@"
                CREATE TABLE IF NOT EXISTS rawticks (
                    ""id"" uuid NOT NULL,
                    ""timestamp"" timestamp with time zone NOT NULL,
                    ""ticker"" character varying(20) NOT NULL,
                    ""price"" numeric(18,8) NOT NULL,
                    ""volume"" numeric(18,8) NOT NULL,
                    ""exchange"" character varying(50) NOT NULL,
                    ""receivedat"" timestamp with time zone NOT NULL,
                    ""normalized"" boolean NOT NULL,
                    CONSTRAINT ""PK_rawticks"" PRIMARY KEY (""id"", ""timestamp"")
                ) PARTITION BY RANGE (""timestamp"");
            ");

            // default-партиция: принимает любые данные, не попадающие в day-партиции,
            // чтобы INSERT-ы никогда не падали из-за отсутствия партиции. Day-партиции
            // создаёт PartitionMaintenanceService (см. PartitioningOptions).
            migrationBuilder.Sql(@"
                CREATE TABLE IF NOT EXISTS rawticks_default
                    PARTITION OF rawticks DEFAULT;
            ");

            migrationBuilder.CreateIndex(
                name: "IX_aggregateddata_starttime",
                table: "aggregateddata",
                column: "starttime");

            migrationBuilder.CreateIndex(
                name: "IX_aggregateddata_ticker_interval",
                table: "aggregateddata",
                columns: new[] { "ticker", "interval" });

            migrationBuilder.CreateIndex(
                name: "IX_connectionlogs_createdat",
                table: "connectionlogs",
                column: "createdat");

            migrationBuilder.CreateIndex(
                name: "IX_connectionlogs_exchange",
                table: "connectionlogs",
                column: "exchange");

            migrationBuilder.CreateIndex(
                name: "IX_rawticks_ticker_exchange_timestamp",
                table: "rawticks",
                columns: new[] { "ticker", "exchange", "timestamp" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "aggregateddata");

            migrationBuilder.DropTable(
                name: "connectionlogs");

            // rawticks создавалась через raw SQL (партиционированная), поэтому
            // удаляем также через SQL. CASCADE убирает зависимые партиции.
            migrationBuilder.Sql("DROP TABLE IF EXISTS rawticks CASCADE;");
        }
    }
}
