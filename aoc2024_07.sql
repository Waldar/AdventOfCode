with cte_data (result, lst) as
(
select /*+ repartition(64) */
       split_part(value, ':', 1)::bigint
     , transform(split(trim(both ' ' from split_part(value, ':', 2)), ' '), t -> t::int)
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_07_input.txt', format => 'text')
  from values ('190: 10 19')
            , ('3267: 81 40 27')
            , ('83: 17 5')
            , ('156: 15 6')
            , ('7290: 6 8 6 15')
            , ('161011: 16 10 13')
            , ('192: 17 8 14')
            , ('21037: 9 7 18 13')
            , ('292: 11 6 16 20') as t (value)
)
  ,  cte_int (result, lst, mat, agg) as
(
select result
     , lst
     , split('1' || lpad(conv(num, 10, 3), array_size(lst)-1, '0'), '') as mat
     , aggregate( lst
                , named_struct('pos', 0, 'val', 0l)
                , (acc, x) -> named_struct( 'pos', acc.pos + 1
                                          , 'val', case mat[acc.pos]
                                                     when '0' then acc.val * x
                                                     when '1' then acc.val + x
                                                     when '2' then acc.val::string || x::string
                                                   end::bigint)
                , acc -> acc.val = result
                ) as agg
  from cte_data
  join lateral explode(sequence(0, power(3, array_size(lst) - 1)::int - 1, 1)) as t (num)
)
  ,  cte_correct as
(
  select result
       , bool_or(agg) filter(where array_position(mat, '2') = 0) as part1
    from cte_int
group by result, lst
  having bool_or(agg)
)
select sum(result) filter(where part1) as part1
     , sum(result)                     as part2
  from cte_correct;
