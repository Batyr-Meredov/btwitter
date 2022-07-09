import time
import numpy as np
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, tuple_factory
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy


class ScyllaAdapter:
    def __init__(self):
        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
            retry_policy=DowngradingConsistencyRetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=60,
            row_factory=tuple_factory,

        )
        self.cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        self.session = self.cluster.connect(keyspace="twitter", wait_for_all_pools=False)
        self.CASSANDRA_PARTITION_NUM = 1500
        self.summary_table = 'summary_2022_01'
        self.ner_table = 'ner_2021_01'

    def split_to_partitions(self, df):
        permuted_indices = np.random.permutation(len(df))
        partitions = []
        for i in range(self.CASSANDRA_PARTITION_NUM):
            partitions.append(df.iloc[permuted_indices[i::self.CASSANDRA_PARTITION_NUM]])
        return partitions

    def write_summary(self, df, info):
        start = time.time()
        prepared_query = self.session.prepare(
            'INSERT INTO summary_2021_12 (sentiment, created, country, tweet_id, ner_text) VALUES (?, ?, ?, ?, ?)'
        )
        executed = 0
        errors = []
        for partition in self.split_to_partitions(df):
            batch = BatchStatement()
            for index, item in partition.iterrows():
                try:
                    batch.add(
                        prepared_query,
                        (
                            item.sentiment,
                            item.created if item.created != 'none' else info.created,
                            item.country,
                            item.tweet_id,
                            item.ner_text if item.ner_text != 'none' else [],
                        )
                    )
                except Exception as e:
                    errors.append(f'batch.add error: {e} - {item.tweet_id} - {item.created}')
            try:
                self.session.execute(batch)
                executed += len(batch)
            except Exception as e:
                errors.append(f'session.execute(batch) error: {e}')

        print(f'write time: {time.time() - start}, df: {df.shape[0]}, executed: {executed}, info: {info}.\n, errors: {errors}')

    def write_ner(self, df, pid):
        start = time.time()
        prepared_query = self.session.prepare(
            'INSERT INTO ner_2021_08 (ner_text, ner_label, created, country, tweet_id) VALUES (?, ?, ?, ?, ?)'
        )
        executed = 0
        errors = []
        for partition in self.split_to_partitions(df):
            batch = BatchStatement()
            for index, item in partition.iterrows():
                try:
                    batch.add(prepared_query, (item.ner_text, item.ner_label, item.created, item.country, item.tweet_id))
                except Exception as e:
                    errors.append(f'batch.add error: {e} - {item.tweet_id} - {item.created}')
            try:
                self.session.execute(batch)
                executed += len(batch)
            except Exception as e:
                errors.append(f'session.execute(batch) error: {e}')

        print(f'errors: {errors}')
        print(f'write time: {time.time() - start}, df: {df.shape[0]}, executed: {executed}, pid: {pid}.\n')
