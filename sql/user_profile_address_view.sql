-- =========================================================================
-- Query 1: User Profile - Current Physical Addresses
-- This query extracts physical address information from the participants table
-- =========================================================================
SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '207908218' AS address_src_question_cid,
    'user_profile_physical_address' AS address_nickname,
    'user_profile' AS address_source,
    0 AS historical_order, -- 1-based position in the array (So current address gets a zero)
    d_207908218 AS address_line_1,
    d_224392018 AS address_line_2,
    NULL AS street_num,    -- there is no street_num for the user profile address since it is given in the address_line_1[2] fields
    NULL AS street_name,   -- there is no street_name for the user profile address since it is given in the address_line_1[2] fields
    NULL AS apartment_num, -- there is no appartment_num for the user profile address since it is given in the address_line_1[2] fields
    d_451993790 AS city,
    d_187799450 AS state,
    d_449168732 AS zip_code,
    NULL AS country, -- there is no country field provided in the user profile
    NULL AS cross_street_1,
    NULL AS cross_street_2
FROM @raw_participants
WHERE
    Connect_ID IS NOT NULL
    AND d_821247024 = 197316935  -- Verification status = verified
    AND d_831041022 = 104430631  -- Data destruction requested = no
    AND d_663265240 = 231311385  -- Module 4 is complete
    AND (
        (d_207908218 IS NOT NULL AND d_207908218 != '') OR
        (d_224392018 IS NOT NULL AND d_224392018 != '') OR
        (d_451993790 IS NOT NULL AND d_451993790 != '') OR
        (d_187799450 IS NOT NULL AND d_187799450 != '') OR
        (d_449168732 IS NOT NULL AND d_449168732 != '')
    )

UNION ALL

-- =========================================================================
-- Query 2: User Profile - Current Mailing Addresses
-- This query extracts mailing address information from participants who don't
-- have physical address information populated (to avoid duplication)
-- =========================================================================
SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '521824358' AS address_src_question_cid,
    'user_profile_mailing_address' AS address_nickname,
    'user_profile' AS address_source,
    0 AS historical_order,  -- 1-based position in the array (So current address gets a zero)
    d_521824358 AS address_line_1,
    d_442166669 AS address_line_2,
    NULL AS street_num,    -- there is no street_num for the user profile address since it is given in the address_line_1[2] fields
    NULL AS street_name,   -- there is no street_name for the user profile address since it is given in the address_line_1[2] fields
    NULL AS apartment_num, -- there is no appartment_num for the user profile address since it is given in the address_line_1[2] fields
    d_703385619 AS city,
    d_634434746 AS state,
    d_892050548 AS zip_code,
    NULL AS country, -- there is no country field provided in the user profile
    NULL AS cross_street_1,
    NULL AS cross_street_2
FROM @raw_participants
WHERE
    Connect_ID IS NOT NULL
    AND d_821247024 = 197316935  -- Verification status = verified
    AND d_831041022 = 104430631  -- Data destruction requested = no
    AND d_663265240 = 231311385  -- Module 4 is complete
    -- Include only records that have at least one mailing address field populated
    AND (
      (d_521824358 IS NOT NULL AND d_521824358 != '') OR
      (d_442166669 IS NOT NULL AND d_442166669 != '') OR
      (d_703385619 IS NOT NULL AND d_703385619 != '') OR
      (d_634434746 IS NOT NULL AND d_634434746 != '') OR
      (d_892050548 IS NOT NULL AND d_892050548 != '')
    )


UNION ALL 

-- =========================================================================
-- Query 3: User Profile History - Physical Addresses
-- This query extracts physical address information from the participants table
-- =========================================================================
SELECT
  CAST(Connect_ID AS STRING) AS Connect_ID,                         
  ts_user_profile_updated,
  CURRENT_TIMESTAMP() AS ts_address_delivered,
  '207908218' AS address_src_question_cid,
  'user_profile_physical_address' AS address_nickname,
  'user_profile' AS address_source,
  (element_position + 1) AS historical_order, -- 1-based position in the array
  address_line_1,
  address_line_2,
  NULL AS street_num,    -- Not available - would need parsing from address_line_1/2
  NULL AS street_name,   -- Not available - would need parsing from address_line_1/2
  NULL AS apartment_num, -- Not available - would need parsing from address_line_1/2
  city,
  state,
  zip_code,
  NULL AS country,       -- Country information not provided in user profile
  NULL AS cross_street_1,
  NULL AS cross_street_2
FROM (
  -- Subquery to extract and prepare the data from the repeated field
  SELECT
    Connect_ID,
    element.d_371303487 AS ts_user_profile_updated,
    element_position,
    -- Physical address fields
    element.d_207908218 AS address_line_1,
    element.d_224392018 AS address_line_2,
    element.d_451993790 AS city,
    element.d_187799450 AS state,
    element.d_449168732 AS zip_code
  FROM
    @raw_participants,
    UNNEST(d_569151507) AS element WITH OFFSET AS element_position
  WHERE 
    Connect_ID IS NOT NULL
    AND d_821247024 = 197316935  -- Verification status = verified
    AND d_831041022 = 104430631  -- Data destruction requested = no
    AND d_663265240 = 231311385  -- Module 4 is complete
    -- Include only records that have at least one physical address field populated
    AND (
        (element.d_207908218 IS NOT NULL AND element.d_207908218 != '') OR
        (element.d_224392018 IS NOT NULL AND element.d_224392018 != '') OR
        (element.d_451993790 IS NOT NULL AND element.d_451993790 != '') OR
        (element.d_187799450 IS NOT NULL AND element.d_187799450 != '') OR
        (element.d_449168732 IS NOT NULL AND element.d_449168732 != '')
    )
)

UNION ALL

-- =========================================================================
-- Query 4: User Profile History - Mailing Addresses
-- This query extracts mailing address information from participants who don't
-- have physical address information populated (to avoid duplication)
-- =========================================================================
SELECT
  CAST(Connect_ID AS STRING) AS Connect_ID,
  ts_user_profile_updated,
  CURRENT_TIMESTAMP() AS ts_address_delivered,
  '521824358' AS address_src_question_cid,
  'user_profile_mailing_address' AS address_nickname,
  'user_profile' AS address_source,
  (element_position + 1) AS historical_order, -- 1-based position in the array
  address_line_1,
  address_line_2,
  NULL AS street_num,    -- Not available - would need parsing from address_line_1/2
  NULL AS street_name,   -- Not available - would need parsing from address_line_1/2
  NULL AS apartment_num, -- Not available - would need parsing from address_line_1/2
  city,
  state,
  zip_code,
  NULL AS country,       -- Country information not provided in user profile
  NULL AS cross_street_1,
  NULL AS cross_street_2
FROM (
  -- Subquery to extract and prepare the data from the repeated field
  SELECT
    Connect_ID,
    element.d_371303487 AS ts_user_profile_updated,
    element_position,
    -- Physical address fields (to check if empty)
    element.d_207908218,
    element.d_224392018,
    element.d_451993790,
    element.d_187799450,
    element.d_449168732,
    -- Mailing address fields
    element.d_521824358 AS address_line_1,
    element.d_442166669 AS address_line_2,
    element.d_703385619 AS city,
    element.d_892050548 AS zip_code,
    element.d_634434746 AS state
  FROM
    @raw_participants,
    UNNEST(d_569151507) AS element WITH OFFSET AS element_position -- Flatten the repeated field (i.e., the array)
  WHERE 
    Connect_ID IS NOT NULL
    AND d_821247024 = 197316935  -- Verification status = verified
    AND d_831041022 = 104430631  -- Data destruction requested = no
    AND d_663265240 = 231311385  -- Module 4 is complete
    -- Only include records that have mailing address data
    AND (
      (element.d_521824358 IS NOT NULL AND element.d_521824358 != '') OR
      (element.d_442166669 IS NOT NULL AND element.d_442166669 != '') OR
      (element.d_703385619 IS NOT NULL AND element.d_703385619 != '') OR
      (element.d_892050548 IS NOT NULL AND element.d_892050548 != '') OR
      (element.d_634434746 IS NOT NULL AND element.d_634434746 != '') 
  ) 
)

UNION ALL

-- =========================================================================
-- Query 5: Alternate Addresses from User Profile in the Participants table
-- =========================================================================

SELECT 
  CAST(Connect_ID AS STRING) AS Connect_ID,
  NULL AS ts_user_profile_updated,
  CURRENT_TIMESTAMP() AS ts_address_delivered,
  '284580415' AS address_src_question_cid,
  'user_profile_alternative_address' AS address_nickname,
  'user_profile' AS address_source,
  0 AS historical_order, -- 1-based position in the array (So current address gets a zero)
  D_284580415 AS address_line_1,
  D_728926441 AS address_line_2,
  NULL AS street_num,      
  NULL AS street_name,      
  NULL AS apartment_num,
  D_907038282 AS city,
  D_970839481 AS state,
  D_379899229 AS zip_code,
  NULL AS country, 
  NULL AS cross_street_1,
  NULL AS cross_street_2
FROM @raw_participants
WHERE 
  Connect_ID IS NOT NULL
  AND d_821247024 = 197316935  -- Verification status = verified
  AND d_831041022 = 104430631  -- Data destruction requested = no
  AND d_663265240 = 231311385  -- Module 4 is complete
  -- Include only if at least one of the address fields is populated
  AND (
      (D_284580415 IS NOT NULL AND D_284580415 != '') OR
      (D_728926441 IS NOT NULL AND D_728926441 != '') OR
      (D_907038282 IS NOT NULL AND D_907038282 != '') OR
      (D_970839481 IS NOT NULL AND D_970839481 != '') OR
      (D_379899229 IS NOT NULL AND D_379899229 != '')
  )

UNION ALL

-- =========================================================================
-- Query 6: Alternate Addresses from User Profile History in the Participants table
-- =========================================================================

SELECT
  CAST(Connect_ID AS STRING) AS Connect_ID,
  ts_user_profile_updated,
  CURRENT_TIMESTAMP() AS ts_address_delivered,
  '284580415' AS address_src_question_cid,
  'user_profile_alternative_address' AS address_nickname,
  'user_profile' AS address_source,
  (element_position + 1) AS historical_order, -- 1-based position in the array
  address_line_1,
  address_line_2,
  NULL AS street_num,    -- Not available - would need parsing from address_line_1/2
  NULL AS street_name,   -- Not available - would need parsing from address_line_1/2
  NULL AS apartment_num, -- Not available - would need parsing from address_line_1/2
  city,
  state,
  zip_code,
  NULL AS country,       -- Country information not provided in user profile
  NULL AS cross_street_1,
  NULL AS cross_street_2
FROM (
  -- Subquery to extract and prepare the data from the repeated field
  SELECT
    Connect_ID,
    element.d_371303487 AS ts_user_profile_updated,
    element_position,
    element.D_284580415 AS address_line_1,
    element.D_728926441 AS address_line_2,
    element.D_907038282 AS city,
    element.D_379899229 AS zip_code,
    element.D_970839481 AS state
  FROM
    @raw_participants,
    UNNEST(d_569151507) AS element WITH OFFSET AS element_position -- Flatten the repeated field (i.e., the array)
  WHERE 
    Connect_ID IS NOT NULL
    AND d_821247024 = 197316935  -- Verification status = verified
    AND d_831041022 = 104430631  -- Data destruction requested = no
    AND d_663265240 = 231311385  -- Module 4 is complete
    -- Only include records that have mailing address data
    AND (
      (element.D_284580415 IS NOT NULL AND element.D_284580415 != '') OR
      (element.D_728926441 IS NOT NULL AND element.D_728926441 != '') OR
      (element.D_907038282 IS NOT NULL AND element.D_907038282 != '') OR
      (element.D_379899229 IS NOT NULL AND element.D_379899229 != '') OR
      (element.D_970839481 IS NOT NULL AND element.D_970839481 != '') 
    )
  ) 
