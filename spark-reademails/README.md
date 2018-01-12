# Data Engineering Coding Exercise for Slack

Greetings and welcome to my homework! Find below my process and rationale for the solutions to this exercise. 

## Approach
I spent a little bit of time manually perusing the dataset itself and the documentation on the CMU site. A few things jumped out:

* I only needed to pay attention to the `.txt` files.  The `.cat` files, while interesting, were not germane to this problem.
* It is documented that the email addresses have already been normalized by the data preparers. I assume that all sender and recipient fields contain only valid emails, even if they're only placeholders derived from other fields. It seemed beyond the scope of this problem to try to normalize beyond this so I'll took these fields at face value.
* Each file is one email. This may be problematic for Hadoop-based solutions.
* The files looked like standard MIME so I should be able to use a regular MIME-parsing library. That's good because I saw also that quoted messages contained header fields which could mess with a simple regex-based parsing strategy. I also have bad memories of manually parsing MIME messages from past projects...
* There just wasn't that much data. The homework dataset is only a few MB. Even the full dataset is only half a million records. There were probably some scale-ignoring shortcuts I could take...
* Speaking of shortcuts, since I only needed the 5 fastest replies I could probably make some assumptions about how long a valid reply takes to arrive.

## Solution 1: A Simple Python Script: read_emails.py
Python has an `email.parser` library because batteries included and of course it does. Given that and the size of the data, I decided to write a proof-of-concept solution as a single, simple python script. This gave me a chance to prototype algorithms, and establish a baseline set of correct results. See comments in that script for more details on the approach. Execute like:

`% ./read_emails.py path/to/unpacked/archive/enron_with_categories/*/*.txt`

On my laptop it completes in a few seconds. 

Question 1 and 2 are easily solved by scanning the records and maintaining a set of maxes and groupings. Since our major goal here was to provide baseline results for more ambitious solutions, Question 3 I solved as literally as possible. The records are sorted and for each record I search backwards until I find a record which exactly matches the requirements for our original-response relationship: "[a reply is] a message from one of the recipients to the original sender whose subject line contains all of the words from the subject of the original email". I limited the length of this backscanning by setting a max response time that I expect the 5 fastest replies to beat. 

## Solution 2: Spark!
The above solution was fine, but it didn't use SQL or Spark or MapReduce or anything else that showcases my BigData skillz. I decided to use this as an excuse to update my dev environment and play with Spark 2.* (I only used Spark 1.5 at my last job).

Run like:

`./spark-reademails/enron_report.sh ~/src/interview/slack-hw-data/enron_with_categories/\*`  (note escaped asterisk)

I've used the Apache Commons mime parser before so I used it again here. 

For file input, I used `sc.wholeTextFiles(...)` for expediency. It is /not/ efficient when used like this. The per-file cost as Spark builds up its splits is way too high. For production code I would probably preprocess the individual files into a small set of sequence files or something similar before handing off to Spark. Or depending on my goals, I might unpack the tarball within Spark itself using Apache Commons /TarArchiveInputStream/.  

To mitigate the performance problems of my easy approach, I dumped the DataFrame from the initial extraction phase into some parquet tables and explored the data with Zeppelin while I refined my algorithms. This sped up development tremendously.

Question 1 and 2 are fairly simple, Question 3 I started to get creative.

Making the assumption that response subjects always look like the subject of the original plus some number of "re: "s let me group the records into threads very simply. Assuming that any given thread is small enough to be processed by one task gave me my first solution, `question3_1()`. This uses a helper function very similar to the original-finding code of the Python solution. 

## Solution 2.1: SparkSQL
That still felt like a Small Data kind of solution to me, so I rewrote that solution in pure SQL for `question3_2()`. That should better handle longer threads although the self-join does worry me a little bit. Depending on how the optimizer plans this query it could turn into an O(n^2) operation. If I were to productionize this query I would start with this approach and then refine, optimize, and play bottleneck whack-a-mole until I met the requirements.