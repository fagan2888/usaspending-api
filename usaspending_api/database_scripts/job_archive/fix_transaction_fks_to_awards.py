"""
Jira Ticket Number(s): DEV-4109

    Use the unique_award_key / generated_unique_award_id to re-link transactions

Expected CLI:

    $ python3 usaspending_api/database_scripts/job_archive/fix_transaction_fks_to_awards.py

Purpose:

    Check the `award_id` FK to `awards.id`
        Make the using transaction_normalized.unique_award_key -> awards.generated_unique_award_id
        If the transaction_normalized.award_id != awards.id:
            Update transaction
"""
import logging
import math
import psycopg2
import time

from os import environ


try:
    CONNECTION_STRING = environ["DATABASE_URL"]
except Exception:
    print("SET env var DATABASE_URL!!!\nTerminating script")
    raise SystemExit(1)


TRANSACTION_UPDATE_FK_SQL = """
UPDATE
    transaction_normalized AS tn
SET
    award_id = a.id
FROM
    awards a
WHERE
        tn.unique_award_key = a.generated_unique_award_id
    AND tn.award_id IS DISTINCT FROM a.id
RETURNING tn.id
"""


class Timer:
    def __init__(self, msg, pipe_output=print):
        self.elapsed = None
        self.end = None
        self.msg = msg
        self.print_func = pipe_output

    def __enter__(self):
        self.start = time.perf_counter()
        self.print_func("Running   {} ...".format(self.msg))
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.print_func("Completed {} in {}".format(self.msg, self.pretty_print(self.elapsed)))

    @staticmethod
    def pretty_print(elapsed):
        f, s = math.modf(elapsed)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return "%d:%02d:%02d.%04d" % (h, m, s, f * 10000)


def main():
    with psycopg2.connect(dsn=CONNECTION_STRING) as connection:
        connection.autocommit = True

        with Timer("", pipe_output=logging.info):
            with connection.cursor() as cursor:
                cursor.execute(TRANSACTION_UPDATE_FK_SQL)
                updated_transactions = [row[0] for row in cursor.fetchall()]

            if updated_transactions:
                row_count = len(updated_transactions)
                logging.info("UPDATED {:,} transaction records".format(row_count))
            else:
                logging.info("#### No transactions to update in range ###")

        logging.info(f"ALL Transaction IDs:\n{','.join([str(i) for i in updated_transactions])}\n\n\n")


if __name__ == "__main__":
    log_format = "[%(asctime)s] [%(levelname)s] - %(message)s"
    logging.getLogger()
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y/%m/%d %H:%M:%S (%Z)")

    with Timer("fix_transaction_fks_to_awards", pipe_output=logging.info):
        main()
