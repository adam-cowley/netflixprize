import threading
import time
import fileinput
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))

# Movies: 4499
# Reviews: 24053765

# Stats
movies = 0
reviews = 0

# Import
movie_id = False
reviews = []

query = """
UNWIND $batch AS row
MERGE (r:Review:NewReview { id: row.movie_id +'--'+ row.user_id })
ON CREATE SET r.rating = toFloat(row.rating) //, r.date = date(row.date)
"""

class ImportThread:
    def __init__(self, session, batch_number, batch):
        self.session = session
        self.batch_number = batch_number
        self.batch = batch


    def run(self):
        self.session.run(query, batch=self.batch)
        

        


class Importer:
    def __init__(self, driver, batch_size=5000):
        # Config
        self.driver = driver
        self.batch_size = batch_size

        self.session = driver.session()

        # Statistics
        self.movies = 0
        self.reviews = 0
        self.batches = 0

        # Batching
        self.movie_id = False
        self.batch = []

    def read_line(self, line):
        if ":" in line:
            self.check_batch()

            self.movies = self.movies + 1

            parts = line.split(":")
            self.movie_id = parts[0]
        else:
            self.reviews = self.reviews + 1

            parts = line.split(",")

            self.batch.append({
                "movie_id": self.movie_id,
                "user_id": parts[0],
                "rating": parts[1],
                "date": parts[2]
            })

    def check_batch(self):
        if len(self.batch) >= self.batch_size:
            self.execute_batch()

    def execute_batch(self):
        self.batches = self.batches + 1

        if self.batches % 100 == 0:
            print("sending batch number", self.batches, len(self.batch), self.reviews)

        task = ImportThread(self.session, self.batches, self.batch)

        task.run()

        # Below is multithreading code - neo don't like it

        #thread = threading.Thread(target=task.run, args=())
        #thread.daemon = True

        # Wait for a new thread
        #while threading.active_count() > 130:
        #    time.sleep(5)



            
        # Start the thread
        #thread.start() 

        

        # Reset Batvh
        self.batch = []

    def import_reviews(self, filename):
        for line in fileinput.input(filename):
            self.read_line(line)

        # Execute Remaining
        self.execute_batch()

        print("Movies ", self.movies)
        print("Ratings", self.reviews)
        print("Batches", self.batches)

    def import_customers(self, filename):
        customers = []

        for line in fileinput.input(filename):
            if "," in line:
                parts = line.split(",")

                customers.append(parts[0])

        print(len(customers))




res = Importer(driver, batch_size=10)
# res.import_reviews("import/combined_data_1.txt")

# print("import/combined_data_2.txt")
# res.import_reviews("import/combined_data_2.txt")

# print("import/combined_data_3.txt")
# res.import_reviews("import/combined_data_3.txt")

# print("import/combined_data_4.txt")
res.import_reviews("import/combined_data_4.txt")