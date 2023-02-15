from prefect import flow, task, get_run_logger
from prefect.filesystems import LocalFileSystem



@task
def add(a, b):
    return  a + b

@task
def sub(a,b):
    return a -b
@task
def mul(a,b):
    prefect_logger = get_run_logger()
    prefect_logger.info("product is now-----", str(a*b))
    return a * b

@task
def change_content(a,b):

    with open("sample.csv", "w") as f:
        f.write(str(a) + str(b))
        f.close()
@flow
def test_flow(a, b):
    k = add(a,b)
    l = sub(a,b)
    m = mul(a,b)
    change_content(a,b)
    with open("result.txt", "w") as f:
        s = "done with this" + str(k+l+m)
        f.write(s)
    return k+l+m

if __name__ == "__main__":
    j = test_flow(10,20)
    print(j)