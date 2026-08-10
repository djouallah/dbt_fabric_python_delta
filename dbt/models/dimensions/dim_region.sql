-- Region entity: the five NEM regions plus the WEM (SWIS).
-- WA1 is a separate market with no interconnection to the NEM, so it is an isolated
-- node in the network graph — keeping it here rather than filtering it out is what
-- makes "which regions can X reach" honest.
SELECT * FROM (VALUES
    ('NSW1', 'New South Wales',   'NEM'),
    ('QLD1', 'Queensland',        'NEM'),
    ('SA1',  'South Australia',   'NEM'),
    ('TAS1', 'Tasmania',          'NEM'),
    ('VIC1', 'Victoria',          'NEM'),
    ('WA1',  'Western Australia', 'WEM')
) AS t(RegionID, State, Market)
