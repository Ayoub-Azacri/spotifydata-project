"""
conftest.py — Configuration pytest pour le projet SPOTIFY

Ce fichier est automatiquement chargé par pytest.
Il configure le path Python pour que les imports src/ fonctionnent.
"""
import sys
import os

# Ajouter la racine du projet au PYTHONPATH
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Configurer AIRFLOW_HOME pour les tests de structure
if "AIRFLOW_HOME" not in os.environ:
    os.environ["AIRFLOW_HOME"] = project_root
