
from Engine.relational import Relational


def main():
    input_str = input("[relational] or [nosql]>").strip()
    if input_str == "relational":
        engine = Relational()
        engine.run()
    elif input_str == "nosql":
        engine = NoSQL()
        engine.run()
        

if __name__ == "__main__":
    main()