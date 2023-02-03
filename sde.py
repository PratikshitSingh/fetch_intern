"""
    Run this script using python3/python using the command: "python3 sde.py 5000",
    where 5000 is the spend amount input passed as argument.
"""

from queue import PriorityQueue

import argparse
import csv

class Transaction:
    def __init__(self, payer,points,timestamp):
        self.payer      = payer
        self.points     = points
        self.timestamp  = timestamp

    # This methodd should return True if the current transaction has an earlier timestamp value than "other"
    # This method will sort the priority queue with sorting w.r.t. timestamps with earliest transaction at top.
    def __lt__(self, other):
        if self.timestamp < other.timestamp:
            return True

    def get_payer(self):
        return self.payer

    def get_points(self):
        return self.points

    def get_timestamp(self):
        return self.timestamp

class ProblemSolver:
    def __init__(self, transaction_filepath):
        self.transaction_filepath   = transaction_filepath
        self.orderedTransactions    = PriorityQueue()
        self.finalTransactions      = PriorityQueue()

        self.createOrderedTransactions()

    def createOrderedTransactions(self):
        with open(self.transaction_filepath) as csvFile:
            reader = csv.reader(csvFile)
            next(reader, None)

            for payer, points, timestamp in reader:
                points  = int(points)
                obj     = Transaction(payer, points, timestamp)
                self.orderedTransactions.put(obj)

            while self.orderedTransactions and not self.orderedTransactions.empty():
                t                           = self.orderedTransactions.get()
                payer, points, timestamp    = t.get_payer(), t.get_points(), t.get_timestamp()
                
                if points > 0:
                    obj = Transaction(payer, points, timestamp)
                    self.finalTransactions.put(obj)
                elif points < 0:
                    while abs(points):
                        final_t = self.finalTransactions.get()
                        if final_t.get_points() - abs(points) > 0:
                            final_t.points  = final_t.points - abs(points)
                            points          = 0
                            self.finalTransactions.put(final_t)
                            break
                        elif final_t.get_points() - abs(points) < 0:
                            points      += final_t.get_points()

    def spendProcessor(self, limit):
        balance_sheet   = {}

        while self.finalTransactions and not self.finalTransactions.empty() and limit > 0:
            t   = self.finalTransactions.get()
            if t.get_points() - limit >= 0:
                t.points                        -= limit
                limit                           = 0
                balance_sheet[t.get_payer()]    = balance_sheet.get(t.get_payer(), 0) + t.points
            else:
                limit                           -= t.get_points()
                t.points                        = 0
                balance_sheet[t.get_payer()]    = balance_sheet.get(t.get_payer(), 0) + t.points

        if limit > 0:
            raise Exception("Spend amount is greater than sum of all available balances")

        while self.finalTransactions and not self.finalTransactions.empty():
            t                            = self.finalTransactions.get()
            balance_sheet[t.get_payer()] = balance_sheet.get(t.get_payer(), 0) + t.points

        return balance_sheet

def main(args):
    spend_amount    = int(args.spend_amount)

    transaction_filepath = "transactions.csv"
    balances = ProblemSolver(transaction_filepath).spendProcessor(spend_amount)
    print(balances)

if __name__ == "__main__":
    """
    Run this script using python3/python using the command: "python3 sde.py 5000"
    where 5000 is the spend amount input passed as argument.
    """
    parser = argparse.ArgumentParser(description='Fetch Software Engineering Internship Exercise')

    # SPEND AMOUNT ARGS
    parser.add_argument('spend_amount', type=str, default=5000, help='This argument is the amount of points to spend')

    args = parser.parse_args()
    main(args)