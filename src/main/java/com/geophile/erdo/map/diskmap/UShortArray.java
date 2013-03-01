package com.geophile.erdo.map.diskmap;

class UShortArray
{
    public UShortArray(int capacity)
    {
        size = 0;
        array = new char[capacity];
    }

    public int size()
    {
        return size;
    }

    public char at(int position)
    {
        return array[position];
    }

    public void append(char x)
    {
        if (size == array.length) {
            char[] newArray = new char[size * 2];
            System.arraycopy(array, 0, newArray, 0, size);
            array = newArray;
        }
        array[size++] = x;
    }

    public void truncate(int newSize)
    {
        size = newSize;
    }

    private int size;
    private char[] array;
}
