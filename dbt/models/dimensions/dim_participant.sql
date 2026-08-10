-- Participant entity: one row per node in the ownership network, with a
-- self-referencing edge to its immediate parent.
--
-- The node set is closed on purpose. It is the union of the participants that
-- actually register units (from dim_duid) and the corporate entities named as
-- parents in dim_participant_parent, most of which register nothing themselves.
-- Without that union, traversing SUBSIDIARY_OF from a registered participant would
-- walk off the end of the graph at the first holding company.
--
-- ParentParticipant is NULL for a root or an owner we could not establish; the two
-- cases are distinguished by IsCurated. No transitive closure here — see the note in
-- dim_participant_parent.
WITH
  registered AS (
    SELECT DISTINCT Participant
    FROM {{ ref('dim_duid') }}
    WHERE Participant IS NOT NULL AND Participant <> ''
  ),

  edges AS (
    SELECT Participant, ParentParticipant FROM {{ ref('dim_participant_parent') }}
  ),

  nodes AS (
    SELECT Participant FROM registered
    UNION
    SELECT Participant FROM edges
    UNION
    SELECT ParentParticipant FROM edges
  )

SELECT
  n.Participant,
  e.ParentParticipant,
  e.Participant IS NOT NULL AS IsCurated,
  r.Participant IS NOT NULL AS IsRegisteredParticipant
FROM nodes n
LEFT JOIN edges e      ON n.Participant = e.Participant
LEFT JOIN registered r ON n.Participant = r.Participant
