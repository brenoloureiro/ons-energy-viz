from setuptools import setup, find_packages

setup(
    name="ons-energy-viz",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "flask==2.3.3",
        "pandas==2.2.3",
        "boto3==1.36.26",
        "plotly==5.16.1",
        "python-dotenv==1.0.0",
        "gunicorn==21.2.0",
        "numpy==1.26.4",
        "scipy==1.12.0",
        "statsmodels==0.14.1",
        "openpyxl==3.1.2",
        "pyarrow==15.0.0",
    ],
) 