with cte_data (lst) as
(
select transform(split(value, ' '), x -> x::tinyint)
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_02_input.txt', format => 'text')
  from values ('7 6 4 2 1')
            , ('1 2 7 8 9')
            , ('9 7 6 2 1')
            , ('1 3 2 4 5')
            , ('8 6 4 4 1')
            , ('1 3 6 7 9') as t (value)
)
  ,  cte_all_lst (lst, lst_f, num) as
(
select lst
     , filter(lst, (x,i) -> i <> u.num) as lst_f
     , u.num
  from cte_data
  join lateral explode(sequence(-1, array_size(lst) - 1, 1)) as u (num)
)
  select count_if(num = -1)  as part1
       , count(distinct lst) as part2
    from cte_all_lst
   where not(array_contains(transform(sequence(1, array_size(lst_f) - 1, 1), i -> abs(lst_f[i] - lst_f[i-1]) between 1 and 3), false))
     and (lst_f = array_sort(lst_f) or lst_f = reverse(array_sort(lst_f)));
