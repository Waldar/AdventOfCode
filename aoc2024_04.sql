with cte_data (id, lst, x_pos, m_pos, a_pos, s_pos) as
(
select monotonically_increasing_id()
     , split(value, '') as lst
     , array_compact(transform(lst, (x, i) -> case x when 'X' then i end))
     , array_compact(transform(lst, (x, i) -> case x when 'M' then i end))
     , array_compact(transform(lst, (x, i) -> case x when 'A' then i end))
     , array_compact(transform(lst, (x, i) -> case x when 'S' then i end))
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_04_input.txt', format => 'text')
  from values ('MMMSXXMASM')
            , ('MSAMXMSMSA')
            , ('AMXSXMAAMM')
            , ('MSAMASMSMX')
            , ('XMASAMXAMM')
            , ('XXAMMXXAMA')
            , ('SMSMSASXSS')
            , ('SAXAMASAAA')
            , ('MAMMMXMMMM')
            , ('MXMXAXMASX') as t (value)
)
  ,  cte_data_prep (id, lst, x_pos, m_pos, a_pos, s_pos, mp1, ap2, sp3, mm1, am2, sm3, sp1, sm1) as
(
select id, lst, x_pos, m_pos, a_pos, s_pos
     , lead(m_pos, 1) over W
     , lead(a_pos, 2) over W
     , lead(s_pos, 3) over W
     , lag (m_pos, 1) over W
     , lag (a_pos, 2) over W
     , lag (s_pos, 3) over W
     , lead(s_pos, 1) over W
     , lag (s_pos, 1) over W
  from cte_data
window W as (order by id asc)
)
select sum(aggregate( x_pos
                    , 0
                    , (acc, x) -> acc
                                + coalesce((array_contains(m_pos, x+1) and array_contains(a_pos, x+2) and array_contains(s_pos, x+3))::tinyint, 0)
                                + coalesce((array_contains(m_pos, x-1) and array_contains(a_pos, x-2) and array_contains(s_pos, x-3))::tinyint, 0)
                                + coalesce((array_contains(mp1  , x  ) and array_contains(ap2  , x  ) and array_contains(sp3  , x  ))::tinyint, 0)
                                + coalesce((array_contains(mm1  , x  ) and array_contains(am2  , x  ) and array_contains(sm3  , x  ))::tinyint, 0)
                                + coalesce((array_contains(mp1  , x+1) and array_contains(ap2  , x+2) and array_contains(sp3  , x+3))::tinyint, 0)
                                + coalesce((array_contains(mp1  , x-1) and array_contains(ap2  , x-2) and array_contains(sp3  , x-3))::tinyint, 0)
                                + coalesce((array_contains(mm1  , x-1) and array_contains(am2  , x-2) and array_contains(sm3  , x-3))::tinyint, 0)
                                + coalesce((array_contains(mm1  , x+1) and array_contains(am2  , x+2) and array_contains(sm3  , x+3))::tinyint, 0)
                    )) as part1
     , sum(aggregate( a_pos
                    , 0
                    , (acc, a) -> acc
                                + coalesce( ((array_contains(mp1, a+1) and array_contains(sm1, a-1) or array_contains(mm1, a-1) and array_contains(sp1, a+1))
                                         and (array_contains(mp1, a-1) and array_contains(sm1, a+1) or array_contains(mm1, a+1) and array_contains(sp1, a-1))
                                  )::tinyint, 0)
                    )) as part2
  from cte_data_prep;
