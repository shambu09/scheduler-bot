"""
Singleton class to store states, returns the same object on every instantiation. 
"""

class Singleton(object):
    _instances = {}

    def __new__(class_, *args, **kwargs):
        if class_ not in class_._instances:
            class_._instances[class_] = super(Singleton, class_).__new__(
                class_, *args, **kwargs
            )
        return class_._instances[class_]
