-- part 1

select aggregate( zip_with( regexp_extract_all(value, r'mul\((\d+),(\d+)\)', 1)
                          , regexp_extract_all(value, r'mul\((\d+),(\d+)\)', 2)
                          , (x , y) -> x::int * y::int
                          )
                , 0
                , (acc, x) -> acc + x
                ) as part1
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_03_input.txt', format => 'text', wholeText => true);
  from values ('xmul(2,4)%&mul[3,7]!@^do_not_mul(5,5)+mul(32,64]then(mul(11,8)mul(8,5))') as t (value);


with cte_do (value) as
(
select explode(split(value, r"do\(\)"))
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_03_input.txt', format => 'text', wholeText => true)
  from values ('xmul(2,4)%&mul[3,7]!@^do_not_mul(5,5)+mul(32,64]then(mul(11,8)mul(8,5))') as t (value)
)
  ,  cte_dont (value) as
(
select split(value, r"don't\(\)")[0]
  from cte_do
)
select sum( aggregate( zip_with( regexp_extract_all(value, r'mul\((\d+),(\d+)\)', 1)
                               , regexp_extract_all(value, r'mul\((\d+),(\d+)\)', 2)
                               , (x , y) -> x::int * y::int
                               )
                     , 0
                     , (acc, x) -> acc + x
                     )
          ) as part2
  from cte_dont;