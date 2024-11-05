#!/bin/python3

s = input('Enter any string : ')

if s.isspace():
    print('All spaces')
elif s.isdigit():
    print('All digits')
elif s.isalpha():
    print('All alphabets')
elif s.islower():
    print('All lowercase letters')
elif s.isupper():
    print('All uppercase letters')
elif s.istitle():
    print('Title case')
elif s.isalnum():
    print('Alphanumeric')
else:
    print('Invalid input')
