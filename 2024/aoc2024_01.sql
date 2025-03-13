with cte_data (l1, l2) as
(
select split_part(value, '   ', 1)::int
     , split_part(value, '   ', 2)::int
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_01_input.txt', format => 'text')
  from values ('3   4')
            , ('4   3')
            , ('2   5')
            , ('1   3')
            , ('3   9')
            , ('3   3') as t (value)
)
  , cte_cnt_l2 (l2, l2_cnt) as
(
  select l2, count(*)
    from cte_data
group by l2
)
    select aggregate( zip_with( array_sort(array_agg(t1.l1))
                              , array_sort(array_agg(t1.l2))
                              , (x, y) -> abs(y - x) )
                    , 0
                    , (acc, x) -> acc + x )    as part1
         , sum(t1.l1 * coalesce(t2.l2_cnt, 0)) as part2
      from cte_data   as t1
 left join cte_cnt_l2 as t2 on t2.l2 = t1.l1;
