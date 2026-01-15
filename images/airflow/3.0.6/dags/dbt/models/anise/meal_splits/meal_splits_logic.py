# ----- PACKAGE DEPENDENCIES ----- #


# Data manipulation
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

# Predictive modeling
import lightgbm as lgb

# Model fine-tuning
import optuna

# Model eval
from sklearn import metrics


# ----- GLOBAL VARIABLES ----- #


# For logging the run timestamp
RUN_TIMESTAMP = pd.Timestamp.utcnow().replace(tzinfo=None)

# Random seed for reproducibility
RAND_SEED = 12345

# Static params for the lightgbm model
STATIC_PARAMS = {
    "verbose": 0,
    "random_state": RAND_SEED,
}

# Number of Optuna optimization trials to run
N_TRIALS = 8

# Timeout seconds for Optuna optimization
N_SEC_TIMEOUT = 500


# ----- MODEL DEFINITION ----- #


def convert_object_to_category(df):
    return df.astype(
        {col: "category" for col in df.select_dtypes(include=["object"]).columns}
    )


def model(dbt, session) -> pd.DataFrame:

    # Model config
    dbt.config(
        materialized="table",
        schema="anise",
        tags=["meal_splits"],
    )

    # Data ingress (DBT model refs)
    meal_splits = dbt.ref("meal_splits_inputs").to_pandas()

    # Lightgbm needs `object` type columns to be converted to `category`
    meal_splits = convert_object_to_category(meal_splits)

    # Specify target and features
    TARGET = "SPLIT"
    FEATURES = [
        i
        for i in meal_splits.columns
        if i not in [TARGET, "MEAL_SKU_ID", "TERM_ID", "CYCLE", "FACILITY_NETWORK"]
    ]

    # Split data into training and inference partitions
    _training = meal_splits[~pd.isnull(meal_splits["SPLIT"])]
    _inference = meal_splits[pd.isnull(meal_splits["SPLIT"])]

    # Split training partition into train/test subpartitions
    X = _training[FEATURES]
    y = _training[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=RAND_SEED,
    )

    def objective_function(trial: optuna.trial.Trial) -> float:

        # Objective function for Optuna
        params = {
            "num_leaves": trial.suggest_int("num_leaves", 20, 150),
            "max_depth": trial.suggest_int("max_depth", 3, 15),
            "learning_rate": trial.suggest_loguniform("learning_rate", 1e-3, 0.3),
            "n_estimators": trial.suggest_int("n_estimators", 50, 1000),
            "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
            "subsample": trial.suggest_uniform("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_uniform("colsample_bytree", 0.5, 1.0),
            "reg_alpha": trial.suggest_loguniform("reg_alpha", 1e-8, 10.0),
            "reg_lambda": trial.suggest_loguniform("reg_lambda", 1e-8, 10.0),
        }

        # Model config
        model = lgb.LGBMRegressor(**STATIC_PARAMS, **params)

        # Model fit
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
        )

        # Generate predictions on held out subpartition
        y_pred = model.predict(X_test)

        # Compute error metrics on held out subpartition
        rmse = metrics.root_mean_squared_error(y_test, y_pred)
        r2 = metrics.r2_score(y_test, y_pred)
        mape = metrics.mean_absolute_percentage_error(y_test, y_pred)

        # Assign metrics to user attr so they can be accessed outside this function
        trial.set_user_attr("rmse", rmse)
        trial.set_user_attr("r2", r2)
        trial.set_user_attr("mape", mape)

        # Objective function returns the value to be minimized
        return rmse

    #
    # Running the optimization with error handling
    study = optuna.create_study(direction="minimize")
    study.optimize(
        objective_function,
        n_trials=N_TRIALS,
        timeout=N_SEC_TIMEOUT,
        callbacks=[],
    )

    # Train final model with best parameters
    best_params = study.best_params
    model = lgb.LGBMRegressor(**STATIC_PARAMS, **best_params)
    model.fit(X_train, y_train)

    # Retain the feature importance weights
    feature_importances = dict(
        zip(
            model.booster_.feature_name(),
            model.booster_.feature_importance(importance_type="gain"),
        )
    )

    # Evaluate on test set
    final_preds = model.predict(_inference[FEATURES])

    # Output table
    cols_to_output = ["MEAL_SKU_ID", "TERM_ID", "CYCLE", "FACILITY_NETWORK"]
    res = _inference[cols_to_output]
    res["PREDICTED_SPLIT"] = final_preds
    res["RUN_TIMESTAMP"] = RUN_TIMESTAMP
    res["METRICS"] = [study.best_trial.user_attrs for _ in range(len(res))]
    res["FEATURE_IMPORTANCE_WEIGHTS"] = [feature_importances for _ in range(len(res))]

    return res
