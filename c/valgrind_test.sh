#!/bin/sh

valgrind --leak-check=full --error-exitcode=11 --num-callers=100 c/$1

