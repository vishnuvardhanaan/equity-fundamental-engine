CREATE TABLE sqlite_stat1(tbl,idx,stat);
CREATE TABLE sqlite_stat4(tbl,idx,neq,nlt,ndlt,sample);
CREATE TABLE raw_equity_universe (
                    SYMBOL TEXT PRIMARY KEY,
                    NAME_OF_COMPANY TEXT,
                    SERIES TEXT,
                    DATE_OF_LISTING DATE,
                    PAID_UP_VALUE INTEGER,
                    MARKET_LOT INTEGER,
                    ISIN_NUMBER TEXT,
                    FACE_VALUE INTEGER,
                    SOURCE TEXT NOT NULL,
                    INGESTED_TS TEXT
                );
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE raw_equity_staticinfo (
            symbol TEXT PRIMARY KEY,
            source TEXT,
            ingested_at TEXT
        , "address1" TEXT, "address2" TEXT, "city" TEXT, "zip" TEXT, "country" TEXT, "phone" TEXT, "fax" TEXT, "website" TEXT, "industry" TEXT, "industrykey" TEXT, "industrydisp" TEXT, "sector" TEXT, "sectorkey" TEXT, "sectordisp" TEXT, "longbusinesssummary" TEXT, "companyofficers" TEXT, "compensationasofepochdate" TEXT, "executiveteam" TEXT, "maxage" TEXT, "currency" TEXT, "tradeable" TEXT, "lastfiscalyearend" TEXT, "nextfiscalyearend" TEXT, "mostrecentquarter" TEXT, "lastsplitfactor" TEXT, "lastsplitdate" TEXT, "quotetype" TEXT, "financialcurrency" TEXT, "language" TEXT, "region" TEXT, "typedisp" TEXT, "quotesourcename" TEXT, "triggerable" TEXT, "custompricealertconfidence" TEXT, "shortname" TEXT, "longname" TEXT, "earningstimestampstart" TEXT, "earningstimestampend" TEXT, "earningscalltimestampstart" TEXT, "earningscalltimestampend" TEXT, "isearningsdateestimate" TEXT, "sourceinterval" TEXT, "exchangedatadelayedby" TEXT, "cryptotradeable" TEXT, "hasprepostmarketdata" TEXT, "firsttradedatemilliseconds" TEXT, "fullexchangename" TEXT, "exchange" TEXT, "messageboardid" TEXT, "exchangetimezonename" TEXT, "exchangetimezoneshortname" TEXT, "gmtoffsetmilliseconds" TEXT, "market" TEXT, "esgpopulated" TEXT, "earningstimestamp" TEXT, "governanceepochdate" TEXT, "irwebsite" TEXT);
CREATE TABLE raw_equity_dynamicinfo (
            symbol TEXT,
            as_of_date TEXT,
            source TEXT,
            ingested_at TEXT,
            extra_payload TEXT, "fulltimeemployees" TEXT, "pricehint" TEXT, "previousclose" TEXT, "open" TEXT, "daylow" TEXT, "dayhigh" TEXT, "regularmarketpreviousclose" TEXT, "regularmarketopen" TEXT, "regularmarketdaylow" TEXT, "regularmarketdayhigh" TEXT, "dividendrate" TEXT, "dividendyield" TEXT, "exdividenddate" TEXT, "payoutratio" TEXT, "beta" TEXT, "trailingpe" TEXT, "volume" TEXT, "regularmarketvolume" TEXT, "averagevolume" TEXT, "averagevolume10days" TEXT, "averagedailyvolume10day" TEXT, "bid" TEXT, "ask" TEXT, "bidsize" TEXT, "asksize" TEXT, "marketcap" TEXT, "fiftytwoweeklow" TEXT, "fiftytwoweekhigh" TEXT, "alltimehigh" TEXT, "alltimelow" TEXT, "pricetosalestrailing12months" TEXT, "fiftydayaverage" TEXT, "twohundreddayaverage" TEXT, "trailingannualdividendrate" TEXT, "trailingannualdividendyield" TEXT, "enterprisevalue" TEXT, "profitmargins" TEXT, "floatshares" TEXT, "sharesoutstanding" TEXT, "heldpercentinsiders" TEXT, "heldpercentinstitutions" TEXT, "impliedsharesoutstanding" TEXT, "bookvalue" TEXT, "pricetobook" TEXT, "earningsquarterlygrowth" TEXT, "netincometocommon" TEXT, "trailingeps" TEXT, "enterprisetorevenue" TEXT, "enterprisetoebitda" TEXT, "f_52weekchange" TEXT, "sandp52weekchange" TEXT, "lastdividendvalue" TEXT, "lastdividenddate" TEXT, "currentprice" TEXT, "recommendationkey" TEXT, "totalcash" TEXT, "totalcashpershare" TEXT, "ebitda" TEXT, "totaldebt" TEXT, "quickratio" TEXT, "currentratio" TEXT, "totalrevenue" TEXT, "debttoequity" TEXT, "revenuepershare" TEXT, "returnonassets" TEXT, "returnonequity" TEXT, "grossprofits" TEXT, "freecashflow" TEXT, "operatingcashflow" TEXT, "earningsgrowth" TEXT, "revenuegrowth" TEXT, "grossmargins" TEXT, "ebitdamargins" TEXT, "operatingmargins" TEXT, "marketstate" TEXT, "averagedailyvolume3month" TEXT, "fiftytwoweeklowchange" TEXT, "fiftytwoweeklowchangepercent" TEXT, "fiftytwoweekrange" TEXT, "fiftytwoweekhighchange" TEXT, "fiftytwoweekhighchangepercent" TEXT, "fiftytwoweekchangepercent" TEXT, "epstrailingtwelvemonths" TEXT, "fiftydayaveragechange" TEXT, "fiftydayaveragechangepercent" TEXT, "twohundreddayaveragechange" TEXT, "twohundreddayaveragechangepercent" TEXT, "regularmarketchangepercent" TEXT, "regularmarketprice" TEXT, "regularmarketchange" TEXT, "regularmarketdayrange" TEXT, "corporateactions" TEXT, "regularmarkettime" TEXT, "trailingpegratio" TEXT, "fiveyearavgdividendyield" TEXT, "forwardpe" TEXT, "forwardeps" TEXT, "targethighprice" TEXT, "targetlowprice" TEXT, "targetmeanprice" TEXT, "targetmedianprice" TEXT, "numberofanalystopinions" TEXT, "epsforward" TEXT, "epscurrentyear" TEXT, "priceepscurrentyear" TEXT, "prevname" TEXT, "namechangedate" TEXT, "recommendationmean" TEXT, "averageanalystrating" TEXT, "auditrisk" TEXT, "boardrisk" TEXT, "compensationrisk" TEXT, "shareholderrightsrisk" TEXT, "overallrisk" TEXT, "newlistingdate" TEXT, "underlyingsymbol" TEXT,
            PRIMARY KEY (symbol, as_of_date)
        );
CREATE INDEX idx_dynamic_symbol_date
        ON raw_equity_dynamicinfo (symbol, as_of_date);
CREATE TABLE raw_info_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            table_name TEXT,
            ingested_at TEXT,
            column_count INTEGER,
            status TEXT
        );
CREATE TABLE raw_stock_balancesheet (
        symbol TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        fiscal_year INTEGER NOT NULL,
        fiscal_date TEXT NOT NULL,
        value REAL NOT NULL,
        source TEXT NOT NULL,
        ingested_ts TEXT NOT NULL,
        last_observed_ts TEXT NOT NULL,
        PRIMARY KEY (symbol, metric_name, fiscal_year)
    );
CREATE INDEX idx_raw_stock_balancesheet_symbol_year
    ON raw_stock_balancesheet(symbol, fiscal_year);
CREATE TABLE raw_stock_incomestmt (
        symbol TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        fiscal_year INTEGER NOT NULL,
        fiscal_date TEXT NOT NULL,
        value REAL NOT NULL,
        source TEXT NOT NULL,
        ingested_ts TEXT NOT NULL,
        last_observed_ts TEXT NOT NULL,
        PRIMARY KEY (symbol, metric_name, fiscal_year)
    );
CREATE INDEX idx_raw_stock_incomestmt_symbol_year
    ON raw_stock_incomestmt(symbol, fiscal_year);
CREATE TABLE raw_stock_cashflowstmt (
        symbol TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        fiscal_year INTEGER NOT NULL,
        fiscal_date TEXT NOT NULL,
        value REAL NOT NULL,
        source TEXT NOT NULL,
        ingested_ts TEXT NOT NULL,
        last_observed_ts TEXT NOT NULL,
        PRIMARY KEY (symbol, metric_name, fiscal_year)
    );
CREATE INDEX idx_raw_stock_cashflowstmt_symbol_year
    ON raw_stock_cashflowstmt(symbol, fiscal_year);
CREATE TABLE raw_statements_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        table_name TEXT NOT NULL,
        ingested_ts TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        status TEXT NOT NULL
    );
