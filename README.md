# sales_hist_etl
Pipeline to ingest and transform sales invoices into Delta tables (silver), then aggregate daily country-wise sales into gold. Supports both full and incremental loads with merge logic for efficient updates.
