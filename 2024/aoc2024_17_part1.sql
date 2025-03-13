with cte_data (registers, program) as
(
select named_struct('A', split(split_part(value, '\n\n', 1), r'[\n|:]')[1]::bigint,
                    'B', split(split_part(value, '\n\n', 1), r'[\n|:]')[3]::bigint,
                    'C', split(split_part(value, '\n\n', 1), r'[\n|:]')[5]::bigint)
     , split(split_part(split_part(value, '\n\n', 2), ': ', 2), ',')::array<tinyint>
--from read_files('/Volumes/waldar/fabien/aoc2024/aoc2024_17_input.txt', format => 'text', wholeText => true) as t (value)
  from values ('Register A: 729' || '\n'
           ||  'Register B: 0'   || '\n'
           ||  'Register C: 0'   || '\n'
           ||  ''                || '\n'
           ||  'Program: 0,1,5,4,3,0') as t (value)
)
select program
     , aggregate( sequence(0, 70, 1) -- 70 is the number of iterations done by the program, there is no conditional exit from this aggregate function, it just do stuff for each array element of the input
                , named_struct('A', registers.A, 'B', registers.B, 'C', registers.C, 'pointer', 0, 'output', array()::array<tinyint>)
                , (acc, x) -> case program[acc.pointer]
                                when 0 then named_struct('A', acc.A div power(2, case program[acc.pointer + 1] when 4y then acc.A when 5y then acc.B when 6y then acc.C else program[acc.pointer + 1] end)::bigint, 'B', acc.B, 'C', acc.C, 'pointer', acc.pointer + 2, 'output', acc.output)
                                when 1 then named_struct('A', acc.A, 'B', acc.B ^ program[acc.pointer + 1] , 'C', acc.C, 'pointer', acc.pointer + 2, 'output', acc.output)
                                when 2 then named_struct('A', acc.A, 'B', case program[acc.pointer + 1] when 4y then acc.A when 5y then acc.B when 6y then acc.C else program[acc.pointer + 1] end % 8, 'C', acc.C, 'pointer', acc.pointer + 2, 'output', acc.output)
                                when 3 then case acc.A when 0 then acc else named_struct('A', acc.A, 'B', acc.B, 'C', acc.C, 'pointer', case program[acc.pointer + 1] when 4y then acc.A when 5y then acc.B when 6y then acc.C else program[acc.pointer + 1] end::int, 'output', acc.output) end
                                when 4 then named_struct('A', acc.A, 'B', acc.B ^ acc.C , 'C', acc.C, 'pointer', acc.pointer + 2, 'output', acc.output)
                                when 5 then named_struct('A', acc.A, 'B', acc.B, 'C', acc.C, 'pointer', acc.pointer + 2, 'output', array_append(acc.output, (case program[acc.pointer + 1] when 4y then acc.A when 5y then acc.B when 6y then acc.C else program[acc.pointer + 1] end % 8)::tinyint))
                                when 6 then named_struct('A', acc.A, 'B', acc.A div power(2, case program[acc.pointer + 1] when 4y then acc.A when 5y then acc.B when 6y then acc.C else program[acc.pointer + 1] end)::bigint, 'C', acc.C, 'pointer', acc.pointer + 2, 'output', acc.output)
                                when 7 then named_struct('A', acc.A, 'B', acc.B, 'C', acc.A div power(2, case program[acc.pointer + 1] when 4y then acc.A when 5y then acc.B when 6y then acc.C else program[acc.pointer + 1] end)::bigint, 'pointer', acc.pointer + 2, 'output', acc.output)
                                else acc
                              end
                , acc -> array_join(acc.output, ',')
                ) as result
  from cte_data;


-- 4,6,3,5,6,3,5,2,1,0
