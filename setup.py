from setuptools import setup

setup(name='newsarchives',
      version='0.1',
      description='News website archival tool',
      url='http://github.com/ahoho/news-archives',
      author='Alexander Hoyle',
      author_email='ahoyle@wesleyan.edu',
      license='MIT',
      packages=['newsarchives'],
      install_requires=[
          'pandas',
          'sqlalchemy',
          'newspaper3k',
          'facebook-sdk',
      ],
      zip_safe=False)