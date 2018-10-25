from setuptools import setup, find_packages
import versioneer


setup(
    name='variant_calling',
    packages=find_packages(),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='variant_calling',
    author='Diljot Grewal',
    author_email='diljot.grewal@gmail.com',
    entry_points={'console_scripts': ['variant_calling = variant_calling.run:main']},
    package_data={'':['scripts/*.py', 'scripts/*.R', 'scripts/*.npz', "config/*.yaml", "data/*"]}
)
