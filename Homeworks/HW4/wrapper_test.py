def print_wrapper(func):
    def wrapper(*args, **kwargs):
        print("Function name: ", func.__name__)
        print("Arguments: ", args)
        print("Keyword arguments: ", kwargs)
        return func(*args, **kwargs)
    return wrapper

@print_wrapper
def test_dag_structure(numbers):
    # Sum the numbers
    return sum(numbers)

test_dag_structure([1, 2, 3, 4, 5])
    