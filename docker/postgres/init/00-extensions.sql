-- Enable pg_stat_statements for query performance monitoring
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- Enable uuid-ossp as a fallback (Prisma uses gen_random_uuid() but belt + suspenders)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
