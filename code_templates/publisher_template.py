from mako.template import Template
import argparse


def generate_task_publisher(task_name):
    template = Template(f"""
@task
def {task_name}(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)
    if not exception_flag:    # if there's no error while executing the task
        # Replace the following with your own code if the need is different. 
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data = df
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline) 
    else:
        # write Exception handling
""")
    return template


if __name__ == "__main__":
    # Create an ArgumentParser object.
    parser = argparse.ArgumentParser(description='Generates task-publisher template')

    # Add arguments to the parser.
    parser.add_argument('--task_name', type=str, help='Name of the task')
    args = parser.parse_args()
    task_name = args.task_name
    template = generate_task_publisher(task_name)
    result = template.render()
    print(result)
