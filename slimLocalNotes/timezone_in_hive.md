[Hive Branch Master][1]

**Hive Schema Setup**:

    ;
**Query 1**:

    -- Time zone is by default 'UTC-8' -> PST

**Query 2**:

    
    select cast('2016-01-01 00:00:00 America/New_York' as timestamp)

**[Results]**:
    |            timestamp |
    |----------------------|
    | 2015-12-31 21:00:00  |
    
**Query 3**:

    select cast('2016-01-01 00:00:00 America/New_York' as timestamp with local time zone)

**[Results]**:
    |          timestamptz |
    |----------------------|
    | 2015-12-31 21:00:00.0 US/Pacific |

**Query 4**:
    
    set time zone UTC

**[Results]**:

**Query 5**:

    
    select cast('2016-01-01 00:00:00 America/New_York' as timestamp)

**[Results]**:
    |            timestamp |
    |----------------------|
    | 2015-12-31 21:00:00  |

**Query 6**:

    
    select cast('2016-01-01 00:00:00 America/New_York' as timestamp with local time zone)

**[Results]**:
    |          timestamptz |
    |----------------------|
    | 2016-01-01 05:00:00.0 UTC |


**Query 7**:

    
    set time zone 'Europe/Madrid'

**[Results]**:


**Query 8**:

    
    select cast('2016-01-01 00:00:00 America/New_York' as timestamp)

**[Results]**:
    |            timestamp |
    |----------------------|
    | 2015-12-31 21:00:00  |

**Query 9**:

    
    select cast('2016-01-01 00:00:00 America/New_York' as timestamp with local time zone)

**[Results]**:
    |          timestamptz |
    |----------------------|
    | 2016-01-01 06:00:00.0 Europe/Madrid |



 [1]: https://github.com/b-slim/hive/tree/696affa2e66b41c1932a8e20ec780d36a7380398
