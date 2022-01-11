import abc
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union

from optuna.distributions import BaseDistribution
from optuna.study import StudyDirection
from optuna.study import StudySummary
from optuna.trial import FrozenTrial
from optuna.trial import TrialState

# from optuna_pyspark import __version__
import optuna

from optuna.storages import BaseStorage
from pyspark.sql import SparkSession

# Study
# study_id : int
# directions : str = Sequence[StudyDirection.MINIMIZE, StudyDirection.MAXIMIZE]
# system attrs : JSON str
# user attrs: JSON str
# study summary

# Trial
# Trial ID / Study ID

class SparkStorage(BaseStorage):

    def __init__(self, spark:SparkSession):
        super().__init__()
        self.spark = None

    def create_new_study(self, study_name: Optional[str] = None) -> int:
        """Create a new study from a name.
        If no name is specified, the storage class generates a name.
        The returned study ID is unique among all current and deleted studies.
        Args:
            study_name:
                Name of the new study to create.
        Returns:
            ID of the created study.
        Raises:
            :exc:`optuna.exceptions.DuplicatedStudyError`:
                If a study with the same ``study_name`` already exists.
        """
        # TODO(ytsmiling) Fix RDB storage implementation to ensure unique `study_id`.
        return 1

    def delete_study(self, study_id: int) -> None:
        """Delete a study.
        Args:
            study_id:
                ID of the study.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        pass

    def set_study_user_attr(self, study_id: int, key: str, value: Any) -> None:
        """Register a user-defined attribute to a study.
        This method overwrites any existing attribute.
        Args:
            study_id:
                ID of the study.
            key:
                Attribute key.
            value:
                Attribute value. It should be JSON serializable.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        pass

    def set_study_system_attr(self, study_id: int, key: str, value: Any) -> None:
        """Register an optuna-internal attribute to a study.
        This method overwrites any existing attribute.
        Args:
            study_id:
                ID of the study.
            key:
                Attribute key.
            value:
                Attribute value. It should be JSON serializable.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        pass

    def set_study_directions(self, study_id: int, directions: Sequence[StudyDirection]) -> None:
        """Register optimization problem directions to a study.
        Args:
            study_id:
                ID of the study.
            directions:
                A sequence of direction whose element is either
                :obj:`~optuna.study.StudyDirection.MAXIMIZE` or
                :obj:`~optuna.study.StudyDirection.MINIMIZE`.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
            :exc:`ValueError`:
                If the directions are already set and the each coordinate of passed ``directions``
                is the opposite direction or :obj:`~optuna.study.StudyDirection.NOT_SET`.
        """
        directions_val = []
        print("xxx", directions)
        for direction in directions:
            print(direction.value)
            directions_val.append(direction.value)
        directions_str = str(directions_val)
        print(directions_str)
        # TODO(ytsmiling) Fix RDB storage implementation to ensure unique `study_id`.


    # Basic study access
    def get_study_id_from_name(self, study_name: str) -> int:
        """Read the ID of a study.
        Args:
            study_name:
                Name of the study.
        Returns:
            ID of the study.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_name`` exists.
        """
        pass

    def get_study_id_from_trial_id(self, trial_id: int) -> int:
        """Read the ID of a study to which a trial belongs.
        Args:
            trial_id:
                ID of the trial.
        Returns:
            ID of the study.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
        """
        return 1

    def get_study_name_from_id(self, study_id: int) -> str:
        """Read the study name of a study.
        Args:
            study_id:
                ID of the study.
        Returns:
            Name of the study.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        return "foo"

    def get_study_directions(self, study_id: int) -> List[StudyDirection]:
        """Read whether a study maximizes or minimizes an objective.
        Args:
            study_id:
                ID of a study.
        Returns:
            Optimization directions list of the study.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        return []

    def get_study_user_attrs(self, study_id: int) -> Dict[str, Any]:
        """Read the user-defined attributes of a study.
        Args:
            study_id:
                ID of the study.
        Returns:
            Dictionary with the user attributes of the study.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        return {}


    def get_study_system_attrs(self, study_id: int) -> Dict[str, Any]:
        """Read the optuna-internal attributes of a study.
        Args:
            study_id:
                ID of the study.
        Returns:
            Dictionary with the optuna-internal attributes of the study.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        return {}

    def get_all_study_summaries(self) -> List[StudySummary]:
        """Read a list of :class:`~optuna.study.StudySummary` objects.
        Returns:
            A list of :class:`~optuna.study.StudySummary` objects.
        """
        return []

    # Basic trial manipulation

    def create_new_trial(self, study_id: int, template_trial: Optional[FrozenTrial] = None) -> int:
        """Create and add a new trial to a study.
        The returned trial ID is unique among all current and deleted trials.
        Args:
            study_id:
                ID of the study.
            template_trial:
                Template :class:`~optuna.trial.FronzenTrial` with default user-attributes,
                system-attributes, intermediate-values, and a state.
        Returns:
            ID of the created trial.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        return 1

    def set_trial_state(self, trial_id: int, state: TrialState) -> bool:
        """Update the state of a trial.
        Args:
            trial_id:
                ID of the trial.
            state:
                New state of the trial.
        Returns:
            :obj:`True` if the state is successfully updated.
            :obj:`False` if the state is kept the same.
            The latter happens when this method tries to update the state of
            :obj:`~optuna.trial.TrialState.RUNNING` trial to
            :obj:`~optuna.trial.TrialState.RUNNING`.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
            :exc:`RuntimeError`:
                If the trial is already finished.
        """
        return False

    def set_trial_param(
        self,
        trial_id: int,
        param_name: str,
        param_value_internal: float,
        distribution: BaseDistribution,
    ) -> None:
        """Set a parameter to a trial.
        Args:
            trial_id:
                ID of the trial.
            param_name:
                Name of the parameter.
            param_value_internal:
                Internal representation of the parameter value.
            distribution:
                Sampled distribution of the parameter.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
            :exc:`RuntimeError`:
                If the trial is already finished.
        """
        pass

    def get_trial_id_from_study_id_trial_number(self, study_id: int, trial_number: int) -> int:
        """Read the trial ID of a trial.
        Args:
            study_id:
                ID of the study.
            trial_number:
                Number of the trial.
        Returns:
            ID of the trial.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``study_id`` and ``trial_number`` exists.
        """
        return 1

    def get_trial_number_from_id(self, trial_id: int) -> int:
        """Read the trial number of a trial.
        .. note::
            The trial number is only unique within a study, and is sequential.
        Args:
            trial_id:
                ID of the trial.
        Returns:
            Number of the trial.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
        """
        return 1

    def get_trial_param(self, trial_id: int, param_name: str) -> float:
        """Read the parameter of a trial.
        Args:
            trial_id:
                ID of the trial.
            param_name:
                Name of the parameter.
        Returns:
            Internal representation of the parameter.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
                If no such parameter exists.
        """
        return 0.0

    def set_trial_values(self, trial_id: int, values: Sequence[float]) -> None:
        """Set return values of an objective function.
        This method overwrites any existing trial values.
        Args:
            trial_id:
                ID of the trial.
            values:
                Values of the objective function.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
            :exc:`RuntimeError`:
                If the trial is already finished.
        """
        pass

    def set_trial_intermediate_value(
        self, trial_id: int, step: int, intermediate_value: float
    ) -> None:
        """Report an intermediate value of an objective function.
        This method overwrites any existing intermediate value associated with the given step.
        Args:
            trial_id:
                ID of the trial.
            step:
                Step of the trial (e.g., the epoch when training a neural network).
            intermediate_value:
                Intermediate value corresponding to the step.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
            :exc:`RuntimeError`:
                If the trial is already finished.
        """
        pass

    def set_trial_user_attr(self, trial_id: int, key: str, value: Any) -> None:
        """Set a user-defined attribute to a trial.
        This method overwrites any existing attribute.
        Args:
            trial_id:
                ID of the trial.
            key:
                Attribute key.
            value:
                Attribute value. It should be JSON serializable.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
            :exc:`RuntimeError`:
                If the trial is already finished.
        """
        pass

    def set_trial_system_attr(self, trial_id: int, key: str, value: Any) -> None:
        """Set an optuna-internal attribute to a trial.
        This method overwrites any existing attribute.
        Args:
            trial_id:
                ID of the trial.
            key:
                Attribute key.
            value:
                Attribute value. It should be JSON serializable.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
            :exc:`RuntimeError`:
                If the trial is already finished.
        """
        pass

    # Basic trial access
    def get_trial(self, trial_id: int) -> FrozenTrial:
        """Read a trial.
        Args:
            trial_id:
                ID of the trial.
        Returns:
            Trial with a matching trial ID.
        Raises:
            :exc:`KeyError`:
                If no trial with the matching ``trial_id`` exists.
        """
        return None

    def get_all_trials(
        self,
        study_id: int,
        deepcopy: bool = True,
        states: Optional[Tuple[TrialState, ...]] = None,
    ) -> List[FrozenTrial]:
        """Read all trials in a study.
        Args:
            study_id:
                ID of the study.
            deepcopy:
                Whether to copy the list of trials before returning.
                Set to :obj:`True` if you intend to update the list or elements of the list.
            states:
                Trial states to filter on. If :obj:`None`, include all states.
        Returns:
            List of trials in the study.
        Raises:
            :exc:`KeyError`:
                If no study with the matching ``study_id`` exists.
        """
        return []


def objective(trial):
    x = trial.suggest_uniform('x', -10, 10)
    score = (x - 2) ** 2
    print('x: %1.3f, score: %1.3f' % (x, score))
    return score

def main():
    spark = SparkSession.builder.appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/testdb.test_collection") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/testdb.test_collection") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

    storage = SparkStorage(spark)
    study = optuna.create_study(storage=storage,study_name="test_study")
    study.optimize(objective, n_trials=2)


if __name__ == '__main__':
    main()
