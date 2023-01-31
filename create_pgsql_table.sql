CREATE TABLE large_trader_fut_qry (
            stdd_oid SERIAL,
            date timestamp,
            prod varchar(6),
            buyer_top5 integer DEFAULT NULL,
            seller_top5 integer DEFAULT NULL,
            create_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            modify_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, prod)
            );

CREATE TABLE fut_daily_market_roprot_mtx (
            stdd_oid SERIAL,
            date timestamp,
            prod varchar(6),
            open_interest integer DEFAULT NULL,
            create_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            modify_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, prod)
            );

CREATE TABLE fut_contracts_date_tfx (
            stdd_oid SERIAL,
            date timestamp,
            prod varchar(6),
            foreign_investors integer DEFAULT NULL,
            investment_trust integer DEFAULT NULL,
            create_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            modify_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, prod)
            );

CREATE TABLE fut_contracts_date_mfx (
            stdd_oid SERIAL,
            date timestamp,
            prod varchar(6),
            insitutional_buy integer DEFAULT NULL,
            insitutional_sell integer DEFAULT NULL,
            create_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            modify_date timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, prod)
            );