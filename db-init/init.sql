-- 1. Create the Stations Lookup Table
CREATE TABLE IF NOT EXISTS stations (
    station_code VARCHAR(10) PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    station_id VARCHAR(5) NOT NULL
);

-- 2. Populate the Static Stations List
INSERT INTO stations (station_code, full_name, station_id) VALUES
('GBT', 'Green Bank (100-m, GBT)', '09'),
('BR',  'Brewster (25-m, VLBA)', '98'),
('HN',  'Hancock (25-m, VLBA)', '91'),
('FD',  'Fort Davis (25-m, VLBA)', '93'),
('KP',  'Kitt Peak (25-m, VLBA)', '96'),
('LA',  'Los Alamos (25-m, VLBA)', '94'),
('MK',  'Mauna Kea (25-m, VLBA)', '99'),
('NL',  'North Liberty (25-m, VLBA)', '92'),
('OV',  'Owens Valley (25-m, VLBA)', '97'),
('PT',  'Pie Town (25-m, VLBA)', '95'),
('SC',  'St. Croix (25-m, VLBA)', '90')
ON CONFLICT (station_code) DO NOTHING; -- Prevents errors if rerun

-- 3. Create the Messages Table for your Dynamic JSON Payloads
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO messages (payload) 
VALUES ('{
  "obs_ID": "obs_001",
  "target": "Moon",
  "xmit_station": "Green Bank (100-m, GBT)",
  "rcvr_station": "North Liberty (25-m, VLBA)",
  "productType": "DDM",
  "productID": "ddm_002",
  "productSource": "DSOC",
  "creationTime": "2026-06-04 15:05:20.103",
  "eventTime": "2026-06-04 14:59:05.096"
}');