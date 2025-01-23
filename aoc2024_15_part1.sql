with cte_data (warehouse, instructions) as
(
select split(split_part(value, '\n\n', 1), '\n')
     , replace(split_part(value, '\n\n', 2), '\n', '')
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_15_input.txt', format => 'text', wholeText => true) as t (value)
  from values ('##########' || '\n'
           ||  '#..O..O.O#' || '\n'
           ||  '#......O.#' || '\n'
           ||  '#.OO..O.O#' || '\n'
           ||  '#..O@..O.#' || '\n'
           ||  '#O#..O...#' || '\n'
           ||  '#O..O..O.#' || '\n'
           ||  '#.OO.O.OO#' || '\n'
           ||  '#....O...#' || '\n'
           ||  '##########' || '\n'
           ||  '' || '\n'
           ||  '<vv>^<v^>v>^vv^v>v<>v^v<v<^vv<<<^><<><>>v<vvv<>^v^>^<<<><<v<<<v^vv^v>^'
           ||  'vvv<<^>^v^^><<>>><>^<<><^vv^^<>vvv<>><^^v>^>vv<>v<<<<v<^v>^<^^>>>^<v<v'
           ||  '><>vv>v^v^<>><>>>><^^>vv>v<^^^>>v^v^<^^>v^^>v^<^v>v<>>v^v^<v>v^^<^^vv<'
           ||  '<<v<^>>^^^^>>>v^<>vvv^><v<<<>^^^vv^<vvv>^>v<^^^^v<>^>vvvv><>>v^<<^^^^^'
           ||  '^><^><>>><>^^<<^^v>>><^<v>^<vv>>v>>>^v><>^v><<<<v>>v<v<v>vvv>^<><<>^><'
           ||  '^>><>^v<><^vvv<^^<><v<<<<<><^v<<<><<<^^<v<^^^><^>>^<v^><<<^>>^v<v^v<v^'
           ||  '>^>>^v>vv>^<<^v<>><<><<v<<v><>v<^vv<<<>^^v^>^^>>><<^v>>v^v><^^>>^<>vv^'
           ||  '<><^^>^^^<><vvvvv^v<v<<>^v<v>v<<^><<><<><<<^^<<<^<<>><<><^^^>^^<>^>v<>'
           ||  '^^>vv<^v^v<vv>^<><v<^v>^^^>>>^^vvv^>vvv<>>>^<^>>>>>^<<^v>^vvv<>^<><<v>'
           ||  'v^^>>><<^^<>>^v^<v^vv<>v^<<>^<^v^v><^<<<><<^<v><v<>vv>>v><v^<vv<>v^<<^') as t (value)
/*from values ('########' || '\n'
           ||  '#..O.O.#' || '\n'
           ||  '##@.O..#' || '\n'
           ||  '#...O..#' || '\n'
           ||  '#.#.O..#' || '\n'
           ||  '#...O..#' || '\n'
           ||  '#......#' || '\n'
           ||  '########' || '\n'
           ||  '' || '\n'
           ||  '<^^>>>vv<v>>v<<') as t (value) */
)
  ,  cte_warehouse_prep1 (wh_lst) as
(
select transform(split(wh, ''), (v, x) -> named_struct('x', x + 1, 'y', row_number() over(order by 1), 'val', v))
  from cte_data
  join lateral explode(warehouse) as t (wh)
)
  ,  cte_warehouse_prep2 (wh_expld) as
(
select explode(wh_lst)
  from cte_warehouse_prep1
)
  ,  cte_warehouse_line (wh_arr) as
(
select array_agg(wh_expld)
  from cte_warehouse_prep2
)
  ,  cte_instr (instr) as
(
select transform( split(instructions, '')
                , t -> named_struct( 'dx' , case t when '<' then -1  when '>' then 1 else 0 end
                                   , 'dy' , case t when '^' then -1  when 'v' then 1 else 0 end ))
  from cte_data
)
  ,  cte_tmp as
(
select aggregate( instr
                , named_struct('rx', filter(wh_arr, y -> y.val = '@')[0].x, 'ry', filter(wh_arr, y -> y.val = '@')[0].y, 'wh', wh_arr)
                , (acc, mov) -> case
                                  when mov.dx <> 0
                                   and filter(acc.wh, z -> z.x = acc.rx + mov.dx and z.y = acc.ry)[0].val <> '#'
                                   and sign( element_at(filter(acc.wh, z -> sign(z.x - acc.rx) = mov.dx and z.y = acc.ry and z.val = '#'), mov.dx).x
                                           - element_at(coalesce(nullif(filter(acc.wh, z -> sign(z.x - acc.rx) = mov.dx and z.y = acc.ry and z.val = '.'), array()), array(named_struct('x', mov.dx * 100, 'y', 0, 'val', '.'))), mov.dx).x )
                                     = mov.dx
                                  then named_struct( 'rx', acc.rx + mov.dx
                                                   , 'ry', acc.ry
                                                   , 'wh', transform(acc.wh, t -> case
                                                                                    when t.val = '@'
                                                                                    then named_struct('x', t.x, 'y', t.y, 'val', '.')
                                                                                    when t.val <> '#'
                                                                                     and t.y = acc.ry
                                                                                     and sign(element_at(filter(acc.wh, z -> sign(z.x - acc.rx) = mov.dx and z.y = acc.ry and z.val = '.'), mov.dx).x - t.x + mov.dx) = mov.dx
                                                                                     and sign(t.x - acc.rx) = mov.dx
                                                                                    then named_struct('x', t.x, 'y', t.y, 'val', filter(acc.wh, z -> z.x = t.x - mov.dx and z.y = t.y)[0].val)
                                                                                    else t
                                                                                  end) )
                                  when mov.dy <> 0
                                   and filter(acc.wh, z -> z.x = acc.rx and z.y = acc.ry + mov.dy)[0].val <> '#'
                                   and sign( element_at(filter(acc.wh, z -> z.x = acc.rx and sign(z.y - acc.ry) = mov.dy and z.val = '#'), mov.dy).y
                                           - element_at(coalesce(nullif(filter(acc.wh, z -> z.x = acc.rx and sign(z.y - acc.ry) = mov.dy and z.val = '.'), array()), array(named_struct('x', 0, 'y', mov.dy * 100, 'val', '.'))), mov.dy).y )
                                     = mov.dy
                                  then named_struct( 'rx', acc.rx
                                                   , 'ry', acc.ry + mov.dy
                                                   , 'wh', transform(acc.wh, t -> case
                                                                                    when t.val = '@'
                                                                                    then named_struct('x', t.x, 'y', t.y, 'val', '.')
                                                                                    when t.val <> '#'
                                                                                     and t.x = acc.rx
                                                                                     and sign(element_at(filter(acc.wh, z -> z.x = acc.rx and sign(z.y - acc.ry) = mov.dy and z.val = '.'), mov.dy).y - t.y + mov.dy) = mov.dy
                                                                                     and sign(t.y - acc.ry) = mov.dy
                                                                                    then named_struct('x', t.x, 'y', t.y, 'val', filter(acc.wh, z -> z.x = t.x and z.y = t.y - mov.dy)[0].val)
                                                                                    else t
                                                                                  end ) )
                                  else acc
                                end
                ) as result
  from cte_warehouse_line
  join cte_instr           on true
)
  select 0 as ord
       , aggregate(filter(agg.wh, z -> z.val = '[')
                  , 0L
                  , (fin, o) -> fin + (o.x - 1) + 100 * (o.y - 1)
                  , fin -> fin::string
                  ) as result
    from cte_tmp
   union all
  select arr.y, array_join(array_agg(arr.val), '')
    from cte_tmp
    join lateral explode(agg.wh) as t (arr)
group by agg.rx, agg.ry, arr.y
order by ord;

/*

2028
########
#....OO#
##.....#
#.....O#
#.#O@..#
#...O..#
#...O..#
########

10092
##########
#.O.O.OOO#
#........#
#OO......#
#OO@.....#
#O#.....O#
#O.....OO#
#O.....OO#
#OO....OO#
##########

*/
