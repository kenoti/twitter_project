How to run this from scratch on EC2:

1. One option is to compile locally into a jar file and run the project
2. Second option is to make sure your machine is updated
3. make sure you have the latest java JDK on the machine
4. make sure you have git installed git
5. you will need to create a hive table with the tweets_hive.hql file shared
6. git clone https://github.com/kenoti/twitter_project.git
7. make sure you have twitter credentials and put them in the 
8. Fill in Twitter credentials twitter4j.properties if running on local or put them into the loadTwitterKeys() before compiling
9. in the main twitter-project folder run mvn package to compile the project
10. Run the compiled jar which should be in the target directory 
11. Run script using 
spark-submit --jars spark-streaming-twitter_2.10-1.6.0.jar,twitter4j-core-4.0.4.jar,twitter4j-stream-4.0.4.jar --class "dsti.Streamer" --master local[8] twitter-project-1.0-SNAPSHOT.jar > tweets_output.txt 2>> tweets_error.txt

to test if you have a properties file put it in the same location as the jar file. This will not work when you use master yarn but will work with master local
12. Run script using spark-submit --class dsti.Streamer --master yarn  twitter-project-1.0-SNAPSHOT-jar-with-dependencies.jar
The application runs for about 3-10 minutes depending on timeout session set collecting tweets

twitter-project-1.0-SNAPSHOT-jar-with-dependencies.jar  has no dependicies loaded so it uses the libraries currently provided on the cluster and this can be problem if there is a conflict in the libraries used in the jar and those on the cluster.

Sample output is below from the cluster. At the using  spark-submit --class dsti.Streamer --master yarn  twitter-project-1.0-SNAPSHOT-jar-with-dependencies.jar
You have a count of tweets collected some text shown and some analysis of some popular hashtags related to football.
Tried to use only english tweets as other languages were tricky to analyse.

14. To read the Jason files to HDFS
hdfs dfs -cat /user/kennedy/tweets/twe*.json/*

15. To read if data is stored in hive go go to hive
hive
show tables;
select * from tweets_hive
data is stored

16. To test the hive system run spark-submit
import org.apache.spark.sql.hive._
val hiveContext = new HiveContext(sc)
hiveContext.sql("SELECT * from tweets_hive").collect().foreach(rdd => println(rdd))
System.out.println("INFO: ****************** Connected in HIVE ******************")
System.out.println("INFO: ****************** End Test Scala ******************")

New tweets 53:

Who Is The Real Athlete?

https://t.co/Ryofj8rzYq

#soccer #football #racing #formulaone #formula1 https://t.co/t8FShDksKh
Wife of #Cowboys QB Tony Romo says his career options go way beyond football »

https://t.co/yzNB0R2Fsz https://t.co/NS2Ie68IPK
RT @BG2MPH: he got the "football player" neck, this must be the year he said fuck the field. https://t.co/Vz8EFQcfve
Wife of #Cowboys QB Tony Romo says his career options go way beyond football »

https://t.co/45UKRpY6hZ https://t.co/7wEX0nA1ZM
RT @DiskiStyle: "What i saw last in Mdantsane was brilliant, great atmosphere. It is important to take football to the people" - @Jimmytau2…
RT @BG2MPH: he got the "football player" neck, this must be the year he said fuck the field. https://t.co/Vz8EFQcfve
#Football Forest owner writes off loans: https://t.co/4q7bS6jqSb
This is fuuuuuuny https://t.co/RiiA2khm7a
@Drop_Tha_Mike LSU has had a winning football team every year this millennium.
RT @Piers_Plowman: Have @piersmorgan and @jk_rowling finished yet? It's like North Korea playing Iran at football. Devil's own job knowing…
Come on #London !! #SFTLondon #football https://t.co/aqPKXeuV1N
RT @TyrellSams: The only player on the team to go play high level Ball in college was the center who went to Ohiost for football lo…
RT @8TrollFootball: Now I have seen everything in football and now I can die in peace. https://t.co/4ReFKDce1c
RT @AndyArmchair: Who else can take you from Tuchel to Wenger to Merson? https://t.co/BlmMS3wL1k
RT @bluecrewnation: The city of Denton wants a Ryan vs. Guyer Football game and we need to make it happen?? RT this so Ryan will see????
RT @Sjopinion10: #rangersfc sorry I forgot to mention the embarrassment Craig whyte ... huge club Scottish football needs them period ....
The House Committee is investigating Donald Trump's security breaches at Mar-a-Lago https://t.co/5JPxmFIeph
RT @SteveDawson0972: hello everyone, bad news, eastenders is cancelled once again, monday 20th february, there will be no eastenders becaus…
*BREAKING NEWS* https://t.co/jzQRueLl9i
RT @JohnnyMcNulty: To compete with Mar-a-Lago giving members selfies w/the nuclear football, Merion Golf Club has acquired a small vial of…
RT @norxbaseball: Good article on @bradcliff9 https://t.co/pihL9jbc03
"Scouts have labeled him 'entitled' and question his football character and leadership."

"...negative feedback fro… https://t.co/k5PMgIc2bN
Bayern Munich vs Arsenal: 11 things you need to know https://t.co/ROzWc8D6H4
There's a reason we're called 'football fans' not 'trophy fans'
Enjoy the football
The rest is gravy https://t.co/G4LlZjABfw
This is what happens when a child tries to do a mans job. I am overwhelmed with so much chaos every single day. https://t.co/s8kBJJVaK2
William Hill 4 Means More offer: Win 25% extra with your football bets! https://t.co/k12c5wWdRv #freebets https://t.co/zDlC8pVFcx
RT @SaintsSouthWest: @ChivenorSoccer u9s lifting the trophy today after a tightly contested tournament. Some unbelievable Football playe…
'Costa is the best pound-for-pound striker in the Premier League' https://t.co/6IlFPXIbSp
RT @iriazbacha: Me loves soccer https://t.co/OwPZNK0EMU
RT @chrisw45: A review of the week's #EPL action via Brazil, Belfast and Siberia - the latest @WFIEPLWeekly  by @WorldFootballi
https://t.…
@TheLoudenTavern Hearing big Eck is going to be the direction of football at the club and that Murty the interm manager into
 The summer !
RT @BG2MPH: he got the "football player" neck, this must be the year he said fuck the field. https://t.co/Vz8EFQcfve
RT @CoachHand: Valentine's Day message to potential prospects.

Love Football, not recruiting.
It's OK to like recruiting, but don't fall i…
RT @THE_BRSO: Pakistani armed forces have abducted two Baloch footballer during a football match in Awaran, abductees are identified as Ban…
Football 'in denial' over link between heading and brain injury https://t.co/JA3avEJDJh #TBI #concussion #braininjury
The 'arms race' continues; FSU considers building new football facility https://t.co/j3kPupWfIJ https://t.co/ROGVzRhpXx
Job Vacancy -  Football Head Coach -Scotland National Women’s ‘A’ Squad - Apply Here https://t.co/ehdP8emxa3 https://t.co/ABn85MkPCN
RT @AspieMom: @thehill @DReaDPiRaTe9 @Morning_Joe This also happened. Trump donor posing with the nuclear football. WTF! https://t.co/yCkTb…
Champions! I just guided Skippingdale to 1st place in The Prem https://t.co/1kKmtH21Lw [Pro Edition]
RT @jgleeson22: Blessed to say that I've received an offer in football from Baylor! https://t.co/HBRDmeGn7O
RT @mykashh_hoe: @mykashh_hoe Bro Football highlights for anyone who would like to see.

https://t.co/ZE555iFEGC
REVEALED: Manchester United made £85m offer for Bayern Munich star this summer

https://t.co/XfBuKipqlS
RT @ManUtdMEN: 'They will get in the Champions League whether by winning the Europa League or finishing in the top four' #mufc…
RT @RyanHannable: Tom Brady to @SI_PeterKing: “Other than playing football the other thing I love to do is prepare to play football."
"One of football's biggest stars", no? ?? https://t.co/kuAIkaN1Yd
RT @BG2MPH: he got the "football player" neck, this must be the year he said fuck the field. https://t.co/Vz8EFQcfve
NFL Cleveland Browns Travel Mug  https://t.co/A4zT7jyLAW #cleveland #browns #football
Cleveland Browns Historic Canvas Drawstring Backpack https://t.co/9xawslEdip #cleveland #browns #football
RT @AthleteFession: Senior football player got leukemia... lost all his hair and got voted best hair. He loved it. -SHS
NEW 2XL-5XL Orange/Gray Cleveland Browns Starter 1/4 Zip #27N Fleece Jacket https://t.co/ABlAGViw11 #cleveland #browns #football
RT @jdassance: Roses are red
Violets are blue
Greyhound Football won at Charlotte
And Basketball did too
#GoHounds
BET 365 NEW CUSTOMER OFFER:

100% DEPOSIT BONUS - UP TO £200

https://t.co/VBv8P8kz1Y

#BET365 #Football… https://t.co/LRVF84QxFg
RT @ltsTheFBLife: RT if you already miss football ????
ReducedUsersCount= 50
NFLWrld (29042 followers)
dbaker242424 (23154 followers)
NFL_Cowboys247 (20998 followers)
WorldFootballi (15722 followers)
SportsJobFinder (9642 followers)
VoiceOfTheStar (6404 followers)
cfcdk (4470 followers)
dave_heller (4288 followers)
teddi_speaks (3549 followers)
Nimsay1872 (2045 followers)
ShawtyGirlKay (1912 followers)
SaLeehMuhammed1 (1770 followers)
mykashh_hoe (1462 followers)
MarketingProRic (1381 followers)
shirleymccay (1114 followers)
Kaye_103 (909 followers)
JasMonae_ (866 followers)
klintarnold (846 followers)
footballgamble1 (608 followers)
AshmanArief (575 followers)
IloveBrahumdagh (573 followers)
dontFINKwithMe (512 followers)
CoachHoneyman (492 followers)
_iamalex8 (471 followers)
TheRealJohnAlty (399 followers)
gilly_beaan (345 followers)
comradegrushko (335 followers)
Sarah__hood (334 followers)
Alonardoroy (317 followers)
maddiemiller__ (305 followers)
free_bets_site (300 followers)
BubbaBB27 (274 followers)
lovo_blanco1 (240 followers)
Rossleet14 (225 followers)
satadru_007 (194 followers)
ChivenorSoccer (180 followers)
Feed4Push (170 followers)
mybraininjury (157 followers)
seanmacinnes30 (118 followers)
Irnestone (114 followers)
ebbywap (97 followers)
ag_goodyear (75 followers)
VictorSwamp (75 followers)
Amd5Ann (56 followers)
Brownsfans6 (54 followers)
williamorris916 (52 followers)
BrodieSullenger (48 followers)
svavavoom (30 followers)
cuntyfuntyucjrh (13 followers)
The_Bark143 (12 followers)

Popular topics in last 3600000 ms seconds (19 total):
#football (5 tweets)
#browns (3 tweets)
#cleveland (3 tweets)
#Cowboys (2 tweets)
#TBI (1 tweets)
#EPL (1 tweets)
#rangersfc (1 tweets)
#SFTLondon (1 tweets)
#Football (1 tweets)
#braininjury (1 tweets)
#formula1 (1 tweets)
#mufc… (1 tweets)
#27N (1 tweets)
#Football… (1 tweets)
#concussion (1 tweets)
#racing (1 tweets)
#formulaone (1 tweets)
#freebets (1 tweets)
#London (1 tweets)
