-- Interconnector entity: the physical edges between NEM regions.
--
-- Hand-maintained reference data, not derived from the AEMO feeds this project
-- ingests (the pipeline loads SCADA and price, not DISPATCHINTERCONNECTORRES).
-- The six in-service rows were checked against the INTERCONNECTORID values actually
-- present in a current PUBLIC_DISPATCHIS report, so they are the real dispatch keys.
--
-- InService matters: EnergyConnect (SA1<->NSW1) is under construction and AEMO has
-- not yet published a dispatch interconnector id for it, so it is carried here as a
-- known-future edge only. Anything traversing this table as a network — a graph
-- binding, a reachability query — must filter on InService, otherwise SA1 gains a
-- direct link to NSW1 that does not exist today and reachability answers go wrong.
--
-- FromRegionID/ToRegionID follow AEMO's positive-flow convention. The link is physically
-- bidirectional; treat the pair as an undirected edge when traversing.
SELECT * FROM (VALUES
    ('V-SA',       'Heywood',               'VIC1', 'SA1',  'AC', TRUE),
    ('V-S-MNSP1',  'Murraylink',            'VIC1', 'SA1',  'DC', TRUE),
    ('T-V-MNSP1',  'Basslink',              'TAS1', 'VIC1', 'DC', TRUE),
    ('VIC1-NSW1',  'VNI',                   'VIC1', 'NSW1', 'AC', TRUE),
    ('NSW1-QLD1',  'QNI',                   'NSW1', 'QLD1', 'AC', TRUE),
    ('N-Q-MNSP1',  'Terranora (Directlink)', 'NSW1', 'QLD1', 'DC', TRUE),
    ('PEC',        'EnergyConnect',         'SA1',  'NSW1', 'AC', FALSE)
) AS t(InterconnectorID, Name, FromRegionID, ToRegionID, AcDc, InService)
