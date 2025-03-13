-- part 1

with cte_data (rules, updates) as
(
select split(split_part(value, '\n\n', 1), '\n')
     , split(split_part(value, '\n\n', 2), '\n')
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_05_input.txt', format => 'text', WholeText => true)
  from values ('47|53'          || '\n'
           ||  '97|13'          || '\n'
           ||  '97|61'          || '\n'
           ||  '97|47'          || '\n'
           ||  '75|29'          || '\n'
           ||  '61|13'          || '\n'
           ||  '75|53'          || '\n'
           ||  '29|13'          || '\n'
           ||  '97|29'          || '\n'
           ||  '53|29'          || '\n'
           ||  '61|53'          || '\n'
           ||  '97|53'          || '\n'
           ||  '61|29'          || '\n'
           ||  '47|13'          || '\n'
           ||  '75|47'          || '\n'
           ||  '97|75'          || '\n'
           ||  '47|61'          || '\n'
           ||  '75|61'          || '\n'
           ||  '47|29'          || '\n'
           ||  '75|13'          || '\n'
           ||  '53|13'          || '\n'
           ||  ''               || '\n'
           ||  '75,47,61,53,29' || '\n'
           ||  '97,61,53,29,13' || '\n'
           ||  '75,29,13'       || '\n'
           ||  '75,97,47,61,53' || '\n'
           ||  '61,13,29'       || '\n'
           ||  '97,13,75,29,47') as t (value)
)
  ,  cte_rules (before, after) as
(
select split_part(r, '|', 1)::tinyint
     , split_part(r, '|', 2)::tinyint
  from cte_data
  join lateral explode(rules) as t (r)
)
  ,  cte_updates (lst, pos, lst_elem) as
(
select transform(split(u, ','), t -> t::tinyint) as lst
     , posexplode(transform(split(u, ','), t -> t::tinyint))
  from cte_data
  join lateral explode(updates) as t (u)
)
  ,  cte_int (lst, med_elem) as
(
  select c.lst
       , element_at(c.lst, ((array_size(c.lst)+1)/2)::int)
    from cte_updates as c
group by c.lst
  having bool_and(not exists(select null from cte_rules as b join cte_updates as t on t.lst_elem = b.after  where b.before = c.lst_elem and t.lst = c.lst and t.pos < c.pos))
)
select sum(med_elem) as part1
  from cte_int;


-- part 2

with cte_data (rules, updates) as
(
select split(split_part(value, '\n\n', 1), '\n')
     , split(split_part(value, '\n\n', 2), '\n')
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_05_input.txt', format => 'text', WholeText => true)
  from values ('47|53'          || '\n'
           ||  '97|13'          || '\n'
           ||  '97|61'          || '\n'
           ||  '97|47'          || '\n'
           ||  '75|29'          || '\n'
           ||  '61|13'          || '\n'
           ||  '75|53'          || '\n'
           ||  '29|13'          || '\n'
           ||  '97|29'          || '\n'
           ||  '53|29'          || '\n'
           ||  '61|53'          || '\n'
           ||  '97|53'          || '\n'
           ||  '61|29'          || '\n'
           ||  '47|13'          || '\n'
           ||  '75|47'          || '\n'
           ||  '97|75'          || '\n'
           ||  '47|61'          || '\n'
           ||  '75|61'          || '\n'
           ||  '47|29'          || '\n'
           ||  '75|13'          || '\n'
           ||  '53|13'          || '\n'
           ||  ''               || '\n'
           ||  '75,47,61,53,29' || '\n'
           ||  '97,61,53,29,13' || '\n'
           ||  '75,29,13'       || '\n'
           ||  '75,97,47,61,53' || '\n'
           ||  '61,13,29'       || '\n'
           ||  '97,13,75,29,47') as t (value)
)
  ,  cte_rules (before, after) as
(
select split_part(r, '|', 1)::tinyint
     , split_part(r, '|', 2)::tinyint
  from cte_data
  join lateral explode(rules) as t (r)
)
  ,  cte_updates (lst, pos, lst_elem) as
(
select transform(split(u, ','), t -> t::tinyint) as lst
     , posexplode(transform(split(u, ','), t -> t::tinyint))
  from cte_data
  join lateral explode(updates) as t (u)
)
  ,  cte_int (lst) as
(
  select /*+ repartition(96) */
         c.lst
    from cte_updates as c
group by c.lst
  having bool_or(exists(select null
                          from cte_rules   as b
                          join cte_updates as t on t.lst_elem = b.after 
                         where b.before = c.lst_elem
                           and t.lst    = c.lst
                           and t.pos    < c.pos))
)
  ,  cte_corrected_lst (lst, rulez, lst_elem_med) as
(
select lst
     , transform(rules, r -> array(split_part(r, '|', 1)::tinyint, split_part(r, '|', 2)::tinyint)) as rulez
     , aggregate( sequence(1, array_size(lst) * 5, 1)
                , lst
                , (acc, x) -> case
                                when array_size(filter(rulez, f -> array_position(acc, f[0]) > array_position(acc, f[1]) and array_position(acc, f[1]) > 0)) > 0
                                then transform(acc, t -> case t
                                                           when filter(rulez, f -> array_position(acc, f[0]) > array_position(acc, f[1]) and array_position(acc, f[1]) > 0)[0][0]
                                                           then filter(rulez, f -> array_position(acc, f[0]) > array_position(acc, f[1]) and array_position(acc, f[1]) > 0)[0][1]
                                                           when filter(rulez, f -> array_position(acc, f[0]) > array_position(acc, f[1]) and array_position(acc, f[1]) > 0)[0][1]
                                                           then filter(rulez, f -> array_position(acc, f[0]) > array_position(acc, f[1]) and array_position(acc, f[1]) > 0)[0][0]
                                                           else t
                                                         end)
                                else acc
                              end
                , acc -> element_at(acc, ((array_size(lst)+1)/2)::int)
                ) as part2
  from cte_int
  join cte_data on true
)
select sum(lst_elem_med) as part2
  from cte_corrected_lst;
