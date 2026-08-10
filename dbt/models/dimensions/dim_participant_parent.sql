-- Curated participant -> immediate parent edges: the recursive SUBSIDIARY_OF
-- relationship. Hand-maintained; this is the ground truth the graph experiment is
-- scored against, so accuracy beats coverage. Participants whose ultimate owner is
-- unclear (joint ventures, entities in administration, single-asset SPVs with no
-- disclosed parent) are deliberately absent rather than guessed — an absent row
-- means "root or unknown", which dim_participant surfaces as a NULL parent.
--
-- Child strings are the exact Participant values from the AEMO registration list
-- after TRIM, which is what dim_duid stores. Parents are corporate entities and are
-- mostly NOT registered participants; dim_participant unions them in as nodes so the
-- chain is closed and traversable.
--
-- Deliberately NOT transitively closed. Each row is one hop. Resolving
-- "AGL including subsidiaries" is exactly the recursive traversal the experiment is
-- measuring, so do not add an ultimate_parent column here — precomputing the closure
-- would hand the star-schema control group the answer and invalidate the test.
SELECT * FROM (VALUES
    -- AGL Energy (listed, root)
    ('AGL Hydro Partnership',                          'AGL Energy Limited'),
    ('AGL SA Generation Pty Limited',                  'AGL Energy Limited'),
    ('AGL Macquarie Pty Limited',                      'AGL Energy Limited'),
    ('AGL Loy Yang Marketing Pty Ltd',                 'AGL Energy Limited'),
    ('AGL Dalrymple Pty Limited',                      'AGL Energy Limited'),
    ('AGL Australia Markets Pty Limited',              'AGL Energy Limited'),
    ('AGL Sales (Queensland Electricity) Pty Limited', 'AGL Energy Limited'),
    ('AGL Liddell BESS Pty Ltd',                       'AGL Energy Limited'),

    -- Origin Energy (listed, root)
    ('Origin Energy Electricity Limited', 'Origin Energy Limited'),

    -- EnergyAustralia -> CLP (Hong Kong)
    ('EnergyAustralia Pty Ltd',          'EnergyAustralia Holdings Limited'),
    ('EnergyAustralia Ecogen Pty Ltd',   'EnergyAustralia Holdings Limited'),
    ('EnergyAustralia Yallourn Pty Ltd', 'EnergyAustralia Holdings Limited'),
    ('EnergyAustralia Holdings Limited', 'CLP Holdings Limited'),

    -- Alinta -> Chow Tai Fook (Hong Kong)
    ('Alinta Energy Retail Sales Pty Ltd', 'Alinta Energy'),
    ('Synergen Power Pty Limited',         'Alinta Energy'),
    ('Alinta Energy',                      'Chow Tai Fook Enterprises Limited'),

    -- Iberdrola (Spain); Lake Bonney arrived via the Infigen acquisition
    ('Iberdrola Australia Energy Markets Pty Ltd', 'Iberdrola Australia Limited'),
    ('Iberdrola Australia Holdings Pty Limited',   'Iberdrola Australia Limited'),
    ('Iberdrola Australia Wallgrove Pty Limited',  'Iberdrola Australia Limited'),
    ('Lake Bonney Wind Power Pty Ltd',             'Iberdrola Australia Limited'),
    ('Iberdrola Australia Limited',                'Iberdrola S.A.'),

    -- Pacific Hydro -> State Power Investment Corporation (China)
    ('Pacific Hydro Investments Pty Ltd',        'Pacific Hydro Pty Ltd'),
    ('Pacific Hydro Clements Gap Pty Ltd',       'Pacific Hydro Pty Ltd'),
    ('Pacific Hydro Clements Gap BESS Pty Ltd',  'Pacific Hydro Pty Ltd'),
    ('Pacific Hydro Challicum Hills Pty Ltd',    'Pacific Hydro Pty Ltd'),
    ('Pacific Hydro Crowlands Pty Ltd',          'Pacific Hydro Pty Ltd'),
    ('Pacific Hydro Yaloak South Pty Ltd',       'Pacific Hydro Pty Ltd'),
    ('Pacific Hydro Haughton Solar Farm Pty Ltd','Pacific Hydro Pty Ltd'),
    ('Pacific Hydro Pty Ltd',                    'State Power Investment Corporation'),

    -- Energy Developments (EDL) -> CK Infrastructure (Hong Kong)
    ('EDL LFG (NSW) Pty Ltd',            'Energy Developments Pty Ltd'),
    ('EDL LFG (Vic) Pty Ltd',            'Energy Developments Pty Ltd'),
    ('EDL LFG (SA) Pty Ltd',             'Energy Developments Pty Ltd'),
    ('EDL LFG (Qld) Pty Ltd',            'Energy Developments Pty Ltd'),
    ('EDL Projects (Australia) Pty Ltd', 'Energy Developments Pty Ltd'),
    ('EDL Group Operations Pty Ltd',     'Energy Developments Pty Ltd'),
    ('EDL CSM (QLD) Pty Ltd',            'Energy Developments Pty Ltd'),
    ('EDL (OCI) Pty Limited',            'Energy Developments Pty Ltd'),
    ('EDL (TT) Pty Limited',             'Energy Developments Pty Ltd'),
    ('Energy Developments Pty Ltd',      'CK Infrastructure Holdings Limited'),

    -- Shell (ex-ERM Power)
    ('Shell Energy Retail Pty Ltd',        'Shell Energy Australia'),
    ('Shell New Energies Australia Pty Ltd','Shell Energy Australia'),
    ('Braemar Power Project Pty Ltd',      'Shell Energy Australia'),
    ('Shell Energy Australia',             'Shell plc'),

    -- Engie (France)
    ('Pelican Point Power Limited',      'ENGIE Australia & New Zealand'),
    ('ENGIE Australia & New Zealand',    'ENGIE SA'),

    -- Neoen (France)
    ('Hornsdale Power Reserve Pty Ltd', 'Neoen SA'),
    ('Bulgana Wind Farm Pty Ltd',       'Neoen SA'),

    -- Government-owned
    ('Snowy Hydro Limited',                 'Commonwealth of Australia'),
    ('Hydro-Electric Corporation',          'Government of Tasmania'),
    ('Stanwell Corporation Limited',        'Government of Queensland'),
    ('CS Energy Limited',                   'Government of Queensland'),
    ('CleanCo Queensland Limited',          'Government of Queensland'),
    ('Ergon Energy Queensland Pty Ltd',     'Energy Queensland Limited'),
    ('Energy Queensland Limited',           'Government of Queensland'),
    ('South Australian Water Corporation',  'Government of South Australia'),
    ('SEC Energy Pty Ltd',                  'Government of Victoria'),

    -- Industrials and other corporates
    ('RTA Yarwun Pty Ltd',              'Rio Tinto Limited'),
    ('Sun Metals Corporation Pty Ltd',  'Korea Zinc Company Limited'),
    ('Tully Sugar Limited',             'COFCO Corporation'),
    ('Mackay Sugar Limited',            'Nordzucker AG'),
    ('Shoalhaven Starches Pty Ltd',     'Manildra Group'),
    ('Manildra Prop Pty Ltd as The Trustee for the Manildra Asset Trust', 'Manildra Group'),
    ('Telstra Energy (Generation) Pty Ltd', 'Telstra Group Limited'),
    ('Enel X Australia Pty Ltd',        'Enel S.p.A.'),
    ('RATCH-Australia Townsville Pty Ltd', 'RATCH Group Public Company Limited'),
    ('Arrow Southern Generation Pty Ltd And Arrow Braemar 2 Pty Ltd', 'Arrow Energy Pty Ltd'),
    ('Victorian Big Battery Pty Ltd as trustee for HMC Energy Transition No. 3 A3 Project Trust',
                                        'HMC Capital Limited')
) AS t(Participant, ParentParticipant)
