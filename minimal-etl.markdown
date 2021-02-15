## Minimalistic-ETL with Clojure

### The Problem

Suppose you are faced with the task to transfer data from one system to another.
Now, suppose that these systems are SQL-friendly and have JDBC-Drivers available. This is still the case in many "enterprise"-environments. The preferred way of interacting with these systems is through interfaces tables or stored procedures.
I know that in theory, everything should/could be done with web-services, but in reality (2010!), just SQL is used most of the time (that is only my limited experience, of course...).


So, if you want to transfer data from system A to system B, you are selecting from some table in A and are inserting into some table from B. Sounds easy, right? Well, so I thought. I did some research on products for enterprise-integration, and it was not easy to find a tool which is capable of doing just that. Instead, you are faced with "solutions" that talk all the time about BPEL, BPMN, SOA and all that, and it is hard to imagine how these tools would help getting this simple job done. (I will compare the solution developed here to a well-known enterprise tool at the end of this post.)

Then I stumbled upon [Scriptella](http://scriptella.javaforge.com/), which seemed ridiculously simple.
Its inventor created a simple DSL for this job; it looks like this:


    <query connection-id="in">
      SELECT * from Bug
        <script connection-id="out">
	      INSERT INTO Bug VALUES (?ID, ?priority, ?summary, ?status);
        </script>
    </query>


Looks extremely simple. We have an outer block named query where the select from the source table resides, and an inner block named script, where can insert into our target table and use variables from the source table.


Now I thought "Hm, that is actually all I need in my project, but can I do it with [Clojure](http://www.clojure.org)?" The
answer is yes, and it takes only a few lines to implement such a DSL!


### A DSL in Clojure
Let's write this DSL. What language elements do I need? Three things:

   1. A syntax for specifying the connections
   2. A syntax for specifying the query from the source table
   3. A syntax for inserting values in the target table

The first task is simple: We do it the way it is done in clojure.contrib.sql:

     (defn connect
       "Creates a JDBC-Connection according to def"
       [def]
       (clojure.contrib.sql.internal/get-connection def))

We can use it like so:

    (def source-def {
      :classname "oracle.jdbc.driver.OracleDriver"
      :subprotocol "oracle:thin"
      :subname "@localhost:1521:xe"
      :user "scott"
      :password "tiger" })

    (def source (connect source-def))

Now we have a connection for the source. We do a similar thing for the target. Let's assume we have done this and bind the connection to target:

    (def target (connect target-def))

Now for the second part, defining a syntax for the query. This will be a macro that takes the connection, the query string and a body of code as arguments. It will then provide the body with a binding *rs* which is set to the sequence of resultsets from the query:

    (defmacro with-query-results
     "Executes query on connection and evaluates body with *rs* bound to the result of
     resultset-seq executed on the result-set."
     [con query body]
       `(with-open [stmt# (.prepareStatement ~con ~query)]
       (with-open [rset# (.executeQuery stmt#)]
       (doseq [~'*rs* (resultset-seq rset#)]
       ~body))))


Now for the final part, the insert into the target table. This is again a macro which takes as parameters the connection, the SQL for the insert and the parameters for the insert. It will then execute the SQL with the given parameters. I also added some code for logging so that the user can figure out where the error, if any, happened:

    (defmacro sql
     [con sql sql-params]
      `(do
         (let [whole-sql-for-logging# (str "SQL: " ~sql ":" ~sql-params)]
	      (clojure.contrib.logging/debug whole-sql-for-logging#)
	         (try
		   (with-open [stmt# (.prepareStatement ~con ~sql)]
	             (doseq [[index# value#] (map vector (iterate inc 1) ~sql-params)]
		     (.setObject stmt# index# value#))
		     (.executeQuery stmt#))
                   (catch Exception e#
                     (do
		        (clojure.contrib.logging/error whole-sql-for-logging# e#)
			(throw e#)))))))

One more utility macro for accessing variables from with-query-results:

    (defmacro v?
    "Returns the value of the column t"
    [t]
      `(~(keyword t) ~'*rs*))

and that's it!

Now let us see this in action:

    (with-query-results source "select * from emp"
      (sql target "insert into employees_names (name) values (?)" [(v? ename)]))

This select everything from the emp table and fills a fictional table employees_names with just the names. Columns from the source table are accessed with the function we defined above: (v? column-name).

And now for the fun-part. The first goodie is about memory consumtion. The resultset-sequence from with-query-results is _lazy_. This means that this simple program does not store the whole result from the query into memory in order to insert it into the target table. Instead, the results are fetched as needed, just when they are accessed. This saves a lot of memory and implies that there is no limit to the size of the table whose data is transferred.

The second is about data-transformation. In the above code, (v? name) is just plain Clojure-code, and we have the full power of the language available! If we wanted only upper-case names in the target table, we could simply type (.toUpperCase (v? ename)) instead. This is possible because we used macros above.

What about DVMs (Domain-Value-Maps)? Just as easy. Say you have a system called A where boolean values are represented by "Y" and "N" and in system B these are represented by "J" and "N". So you want to translate "Y" to "J" and "N" to "N". Define a function

     (defn apply-dvm
     "Domain-Value-Map. Usage:
     (apply-dvm the-dvm-map :system-a :system-b 'Y')"
     [dvm from to txt]
     	  (let [res (filter #(= txt (from %)) dvm)]
   	  (cond
     	  (= 0 (count res)) (throwf "No match: DVM=%s, From=%s, Text=%s" dvm from txt)
     	  (> 1 (count res)) (throwf "More than one match: DVM=%s, From=%s, Text=%s" dvm from txt)
     	     true (to (first res)))))

Then define the DVM itself:

     (def dvm-bool
     [{:system-a "Y" :system-b "J"}
      {:system-a "N" :system-b "N"}])

Now to use the DVM you just type (apply-dvm dvm-bool (v? my-Y-N-column)).

### Efficiency
We have written a little bit of Clojure code that allows us to perform complex etl jobs.
Why was that so simple? After all, there are many etl-tools on the market that are itself immensely complex. It is unbelievable that so little code can perfom equivalent jobs. Why is this so?
My explanation is that these tools invent new languages that are artificially restricted to accomplish this task (Does anyone know BPEL?). Most likely, the payload from the database will be transformed to XML in aproprietary fashion,
you will code the transformation (the essence of the problem) in XSLT, and the resulting XML will be fed into the target database, again in a proprietary fashion.
In addition you have to provide BPEL-Code to "orchestrate" the integration-flow.
And then you will have to provide some glue configuration that keeps all of this together. Some tools even communicate with webservices internally, exposing you to the complexities of that technology too.
All of these introduce artificial breaks in your code. I have tried to accomplish the above task with some state-of-the-art software.
And believe me: There are a lot of things that can go wrong with BPEL, XSLT, WSDL and the glue configuration,
and you can spend hours and hours to fix problems that do not have anything to do with your original problem, which is just to transfer some data.
This is because you constantly have to switch between SQL, proprietary database-adapter configuration, XML, XSLT, BPEL, WSDL (if you are extremely unlucky), and lots of glue-configuration.

Now it is time to take a step back. If all you want to achive is just transferring data from table to table, you will NOT need all of this. After all, it is just selecting from a table, doing some transformations and then inserting into another table.
*How complex do you want to make this?*
With the above solution, you are actually doing all of your coding in ONE language, Clojure. This would not have been possible in a language without a real macro-system. With such a language, there is no need to use an artificially restricted language, but you can use full Clojure everywhere - there is no need switch between different technologies, and this allows you to keep focus on the real task without wasting time to make your tools happy.

This is where productivity comes from.
