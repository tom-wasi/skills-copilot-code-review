"""
MongoDB database configuration and setup for Mergington High School API
"""

from copy import deepcopy
from pymongo import MongoClient
from argon2 import PasswordHasher, exceptions as argon2_exceptions


class _UpdateResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


class InMemoryCollection:
    def __init__(self):
        self._documents = {}

    @staticmethod
    def _get_nested(doc, path: str):
        value = doc
        for part in path.split('.'):
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
        return value

    @classmethod
    def _matches(cls, doc, query):
        if not query:
            return True

        for key, condition in query.items():
            value = cls._get_nested(doc, key)

            if isinstance(condition, dict):
                if "$in" in condition:
                    options = condition["$in"]
                    if isinstance(value, list):
                        if not any(item in options for item in value):
                            return False
                    elif value not in options:
                        return False
                if "$gte" in condition and (value is None or value < condition["$gte"]):
                    return False
                if "$lte" in condition and (value is None or value > condition["$lte"]):
                    return False
            else:
                if value != condition:
                    return False

        return True

    def count_documents(self, query):
        return len(list(self.find(query)))

    def insert_one(self, doc):
        self._documents[doc["_id"]] = deepcopy(doc)

    def find(self, query=None):
        for doc in self._documents.values():
            if self._matches(doc, query or {}):
                yield deepcopy(doc)

    def find_one(self, query):
        for doc in self.find(query):
            return doc
        return None

    def update_one(self, filter_query, update):
        target_id = filter_query.get("_id")
        if target_id is None or target_id not in self._documents:
            return _UpdateResult(0)

        doc = self._documents[target_id]
        modified = False

        if "$push" in update:
            for key, value in update["$push"].items():
                if key not in doc or not isinstance(doc[key], list):
                    doc[key] = []
                doc[key].append(value)
                modified = True

        if "$pull" in update:
            for key, value in update["$pull"].items():
                if key in doc and isinstance(doc[key], list) and value in doc[key]:
                    doc[key].remove(value)
                    modified = True

        return _UpdateResult(1 if modified else 0)

    def aggregate(self, pipeline):
        documents = list(self.find({}))

        for stage in pipeline:
            if "$unwind" in stage:
                path = stage["$unwind"].lstrip("$")
                unwound = []
                for doc in documents:
                    values = self._get_nested(doc, path)
                    if not isinstance(values, list):
                        continue
                    for value in values:
                        copy_doc = deepcopy(doc)
                        target = copy_doc
                        parts = path.split('.')
                        for part in parts[:-1]:
                            target = target[part]
                        target[parts[-1]] = value
                        unwound.append(copy_doc)
                documents = unwound

            elif "$group" in stage:
                group_id = stage["$group"]["_id"].lstrip("$")
                unique_values = set()
                for doc in documents:
                    unique_values.add(self._get_nested(doc, group_id))
                documents = [{"_id": value} for value in unique_values if value is not None]

            elif "$sort" in stage:
                sort_key, direction = next(iter(stage["$sort"].items()))
                reverse = direction == -1
                documents = sorted(
                    documents,
                    key=lambda item: (item.get(sort_key) is None, item.get(sort_key)),
                    reverse=reverse,
                )

        for doc in documents:
            yield doc


def _create_collections():
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=1000)
        client.admin.command("ping")
        db = client['mergington_high']
        return db['activities'], db['teachers']
    except Exception:
        return InMemoryCollection(), InMemoryCollection()


activities_collection, teachers_collection = _create_collections()

# Methods


def hash_password(password):
    """Hash password using Argon2"""
    ph = PasswordHasher()
    return ph.hash(password)


def verify_password(hashed_password: str, plain_password: str) -> bool:
    """Verify a plain password against an Argon2 hashed password.

    Returns True when the password matches, False otherwise.
    """
    ph = PasswordHasher()
    try:
        ph.verify(hashed_password, plain_password)
        return True
    except argon2_exceptions.VerifyMismatchError:
        return False
    except Exception:
        # For any other exception (e.g., invalid hash), treat as non-match
        return False


def init_database():
    """Initialize database if empty"""

    # Initialize activities if empty
    if activities_collection.count_documents({}) == 0:
        for name, details in initial_activities.items():
            activities_collection.insert_one({"_id": name, **details})

    # Initialize teacher accounts if empty
    if teachers_collection.count_documents({}) == 0:
        for teacher in initial_teachers:
            teachers_collection.insert_one(
                {"_id": teacher["username"], **teacher})


# Initial database if empty
initial_activities = {
    "Chess Club": {
        "description": "Learn strategies and compete in chess tournaments",
        "schedule": "Mondays and Fridays, 3:15 PM - 4:45 PM",
        "schedule_details": {
            "days": ["Monday", "Friday"],
            "start_time": "15:15",
            "end_time": "16:45"
        },
        "max_participants": 12,
        "participants": ["michael@mergington.edu", "daniel@mergington.edu"]
    },
    "Programming Class": {
        "description": "Learn programming fundamentals and build software projects",
        "schedule": "Tuesdays and Thursdays, 7:00 AM - 8:00 AM",
        "schedule_details": {
            "days": ["Tuesday", "Thursday"],
            "start_time": "07:00",
            "end_time": "08:00"
        },
        "max_participants": 20,
        "participants": ["emma@mergington.edu", "sophia@mergington.edu"]
    },
    "Morning Fitness": {
        "description": "Early morning physical training and exercises",
        "schedule": "Mondays, Wednesdays, Fridays, 6:30 AM - 7:45 AM",
        "schedule_details": {
            "days": ["Monday", "Wednesday", "Friday"],
            "start_time": "06:30",
            "end_time": "07:45"
        },
        "max_participants": 30,
        "participants": ["john@mergington.edu", "olivia@mergington.edu"]
    },
    "Soccer Team": {
        "description": "Join the school soccer team and compete in matches",
        "schedule": "Tuesdays and Thursdays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Tuesday", "Thursday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 22,
        "participants": ["liam@mergington.edu", "noah@mergington.edu"]
    },
    "Basketball Team": {
        "description": "Practice and compete in basketball tournaments",
        "schedule": "Wednesdays and Fridays, 3:15 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Wednesday", "Friday"],
            "start_time": "15:15",
            "end_time": "17:00"
        },
        "max_participants": 15,
        "participants": ["ava@mergington.edu", "mia@mergington.edu"]
    },
    "Art Club": {
        "description": "Explore various art techniques and create masterpieces",
        "schedule": "Thursdays, 3:15 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Thursday"],
            "start_time": "15:15",
            "end_time": "17:00"
        },
        "max_participants": 15,
        "participants": ["amelia@mergington.edu", "harper@mergington.edu"]
    },
    "Drama Club": {
        "description": "Act, direct, and produce plays and performances",
        "schedule": "Mondays and Wednesdays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Monday", "Wednesday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 20,
        "participants": ["ella@mergington.edu", "scarlett@mergington.edu"]
    },
    "Math Club": {
        "description": "Solve challenging problems and prepare for math competitions",
        "schedule": "Tuesdays, 7:15 AM - 8:00 AM",
        "schedule_details": {
            "days": ["Tuesday"],
            "start_time": "07:15",
            "end_time": "08:00"
        },
        "max_participants": 10,
        "participants": ["james@mergington.edu", "benjamin@mergington.edu"]
    },
    "Debate Team": {
        "description": "Develop public speaking and argumentation skills",
        "schedule": "Fridays, 3:30 PM - 5:30 PM",
        "schedule_details": {
            "days": ["Friday"],
            "start_time": "15:30",
            "end_time": "17:30"
        },
        "max_participants": 12,
        "participants": ["charlotte@mergington.edu", "amelia@mergington.edu"]
    },
    "Weekend Robotics Workshop": {
        "description": "Build and program robots in our state-of-the-art workshop",
        "schedule": "Saturdays, 10:00 AM - 2:00 PM",
        "schedule_details": {
            "days": ["Saturday"],
            "start_time": "10:00",
            "end_time": "14:00"
        },
        "max_participants": 15,
        "participants": ["ethan@mergington.edu", "oliver@mergington.edu"]
    },
    "Science Olympiad": {
        "description": "Weekend science competition preparation for regional and state events",
        "schedule": "Saturdays, 1:00 PM - 4:00 PM",
        "schedule_details": {
            "days": ["Saturday"],
            "start_time": "13:00",
            "end_time": "16:00"
        },
        "max_participants": 18,
        "participants": ["isabella@mergington.edu", "lucas@mergington.edu"]
    },
    "Sunday Chess Tournament": {
        "description": "Weekly tournament for serious chess players with rankings",
        "schedule": "Sundays, 2:00 PM - 5:00 PM",
        "schedule_details": {
            "days": ["Sunday"],
            "start_time": "14:00",
            "end_time": "17:00"
        },
        "max_participants": 16,
        "participants": ["william@mergington.edu", "jacob@mergington.edu"]
    }
}

initial_teachers = [
    {
        "username": "mrodriguez",
        "display_name": "Ms. Rodriguez",
        "password": hash_password("art123"),
        "role": "teacher"
    },
    {
        "username": "mchen",
        "display_name": "Mr. Chen",
        "password": hash_password("chess456"),
        "role": "teacher"
    },
    {
        "username": "principal",
        "display_name": "Principal Martinez",
        "password": hash_password("admin789"),
        "role": "admin"
    }
]
