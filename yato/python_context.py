import importlib.util


def load_class_from_file_path(file_path, class_name) -> object:
    """
    Load a class dynamically from a file path.
    :param file_path: Path of the file to dynamically import.
    :param class_name: The name of the Class to load.
    :return: Return the Class object.
    """
    spec = importlib.util.spec_from_file_location("module.name", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, class_name)
