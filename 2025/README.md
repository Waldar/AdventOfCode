This is my take on [Advent of Code 2025](https://adventofcode.com/2025).<br>

I've 100% completed this challenge using Databricks SQL, all (but day 10) run with one query to compute all of it (parsing, part1 and part2).<br>
No table, functions or views, just plain SQL on the files for inputs.<br>
Usually with Databricks we prefer to tackle massive amounts of data with not too complex algorithms.<br>
Here it's the opposite, few data but complex algorithms!<br>

Every solution runs in the [Databricks Free Edition](https://www.databricks.com/fr/learn/free-edition) with a one node 2XS cluster, feel free to try it!<br>
Check the setup queries to create a catalog, a schema and a volume. Upload your inputs in this volume.<br>
From queries you'll need to update the read_files part with your catalog, schema, volume and file names.<br>

Beware, git display of databricks ipynb extension is not perfect, but it's only the display.<br>
If you have a doubt with what your seeing check the raw code.<br>

Most of the solutions relies on the databricks specific [Aggregate](https://docs.databricks.com/aws/en/sql/language-manual/functions/aggregate) / [Reduce](https://docs.databricks.com/aws/en/sql/language-manual/functions/reduce) function.<br>
This function allows to parse an array and do more or less whatever you want while at it, using an accumulator that can be of any type to held intermediate results.<br>
While it's extremelly powerful, it has also some flows: no break logic, not photonized (for this kind of puzzles it's not an issue), and can get super complex to understand (especially when you don't comment the code... dummy me).<br>

All my solutions are set based, but leveraging the previous function make it looks less SQLy than DuckDB solutions for example.<br>

From a performance PoV - they aren't competitive with C, C++, Rust... solutions that run in μs.<br>
But the code should scale to million of rows :-)<br>
To get the compute time and aleviate some of the cloud burden, I've run the queries five times.<br>
I've ignored the first twoo, and took the average of the Tasks total time in the query profile from the remaining runs.<br>
As all queries run in a single task I think it's fair enough.<br>

Breakdown per day

**Day 1** - 153 ms<br>
Part one was quite easy in SQL, simply compute a running sum with a positive modulo on it and checking when it landed on a zero.<br>
Part two was not too hard, but it took me way too long to find the proper expression when rotating to the left.<br>

**Day 2** - 188 ms<br>
For this one I first landed the super easy regexp syntax with our pipe syntax which I found quite elegant here.<br>
Further optimizations were done to the solution using more complex arithmetics, and I 100% used content from reddit (users and post credited in the file).<br>

**Day 3** - 211 ms<br>
Not too hard once you get that the largest number are done from sliding windows.<br>

**Day 4** - 304 ms<br>
Counting the adjacent values for part 1, then doing that recursively for part 2.<br>

**Day 5** - 289 ms<br>
Part 2 couldn't be done by brute force in a decent amount of time. It was mostly interval merging, nothing too hard here.<br>

**Day 6** - 750 ms<br>
This one was fun. Parsing the numbers horizontaly of verticaly, keeping or ignoring space, most of the code is too actually ties the strings to the operator.<br>
Processing part1 and part2 once it's done wasn't too difficult.<br>

**Day 7** - 204 ms<br>
Parsing the puzzle one step at a time, keeping track of number of separators hit for part1, and also aggregating beams from previous levels for part 2.<br>
This computes fast but the code starts to look fuzzy.<br>

**Day 8** - 1287 ms<br>
The complexity here was mostly the merge logic of segments, other than that no particular difficulty.<br>
Code would run faster if I could break part 2 parsing once the criteras are met, but it is what it is.<br>

**Day 9** - 2237 ms<br>
Funnily enough, this puzzle seemed difficult in the classical language to solve.<br>
By reading the topic I immediately thought of using geospatial capabilities to solve it.<br>
I've let the naive in the code (commented), I noticed that most of the compute was going into checking if the polygon contained the minimum bounding rectangle of the points.<br>
So except of computing all combinations, I've sorted my candidates by area in descending order, and the first rectangle to match the critera would be the largest.<br>
This allowed me to compute only half the combinations againts the polygon, which was twice faster than the initial approach.<br>

**Day 10** - part 1: 214 ms - part 2: 6451 ms<br>
This was THE highlight of this year AdventOfCode.<br>
Part 1 wasn't so bad once you understood it was running some XOR until a condition is met.<br>
Part 2 was a freaking integral linear programming problem. This is a NP-complete problem, no way to brute force that.<br>
Well I studied that in maths almost 30 years ago, and I just remember the name simplex but actually nothing that I should do.<br>

And I did ask for help here to LLMs. Not to provide a full solution, just to provide guidance here.<br>
Funnily enough, ChatGPT discouraged me to even try to solve this in SQL.<br>
Perplexity was more helpful but it was mostly brute force compute with branching and cutting as early as possible, which didn't work here.<br>
Finally, I reverted to cursor, who said, and I quote : " Ah, a classic Set Cover / Integer Linear Programming problem disguised as a SQL challenge! Love it! 🧠 ".<br>
And I prompted and prompted for hours :-) But I landed a working solution, with the Gauss-Jordan matrix reduction, remaining free variables try all combinations within computed bounds, then extract working solutions with the lowest cost.<br>
Once cursor put the bricks, I reviewed and optimized all the code and made it twice faster.<br>

I had to split part1 and part2 here, when I tried to run them in a single query the optimizer is messing up and compute time explodes for no reason.<br>
Still, having a way to solve an ILP within seconds was a major win for me (shamelessly assisted by cursor).<br>

**Important note** I've built this code to solve this challenge. It won't solve all ILP problems.<br>
For example I've hard coded only three free variables, if more are needed the code should be adapted.<br>

**Day 11** - 1267 ms<br>
This one was also interesting but less difficult than the previous one. In SQL, it's still not an easy problem to traverse a hierarchy one element at a time and keep track of intermediates metrics.<br>
Usually in SQL when we run a recursive query, it's always BFS. Here we needed DFS + Memoization.<br>
Note while I wrote most of the code alone, I did ask cursor to correct me at some point because I messed up in tracking index in arrays.<br>
Once everything was fixed it was a quite efficient solution.<br>

**Day 12** - 191 ms<br>
Thanks to the author to make it an Easter egg here :-)<br>
It could have been super complex but at the end it was easy.<br>

