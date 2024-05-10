#!/usr/bin/env python

import os
from pathlib import Path
import argparse

from app_recipe import AppRecipe

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('recipe_file')
    args = parser.parse_args()

    recipeFilename = args.recipe_file
    if os.path.isfile(recipeFilename):
        print('Recipe filename: {0}'.format(recipeFilename))
    else:
        currentPath = Path(__file__).parent.absolute()
        recipeFilename = os.path.join(currentPath, 'deployment-instances', recipeFilename)
        if os.path.isfile(recipeFilename):
            print('Recipe filename: {0}'.format(recipeFilename))
        else:
            raise FileExistsError(f'Could not find the file for recipe: {recipeFilename}')

    appRecipe = AppRecipe(recipeFilename)
    appRecipe.outputScriptsFiles()

if __name__ == "__main__":
    main()

