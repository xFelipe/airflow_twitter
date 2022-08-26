from datetime import datetime, timedelta
import json
from pathlib import Path
import os

from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook

class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    @apply_defaults
    def __init__(
        self, query, file_path,
        conn_id=None, start_time=None,
        end_time=None,  *args, **kwargs
        ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def _create_parent_folter(self):
        folder = Path(Path(self.file_path).parent)
        folder.mkdir(parents=True, exist_ok=True)


    def execute(self, context):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time
        )
        self._create_parent_folter()
        with open(self.file_path, 'w') as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write('\n')

if __name__ == "__main__":
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            query="AluraOnline",
            file_path=os.path.join(
                "/home/felipe/projetos/estudos/datapipeline/datalake",
                "twitter_aluraonline",
                "extract_date={{ ds }}",
                "AluraOnline_{{ ds_nodash }}.json"
            ),
            task_id="test_run"
        )
        ti = TaskInstance(task=to, execution_date=datetime.now()- timedelta(days=1))
        ti.run()
        #to.execute(context=ti.get_template_context())