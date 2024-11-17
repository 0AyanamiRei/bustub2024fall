#include <iostream>
#include <cstring>

using namespace std;

bool flag = true;
const char* input;
int pos = 0;

char getToken() {
    return input[pos++];
}

void E();
void T();
void F();

void E() {
    T();
    while (input[pos] == '+') {
        getToken();
        T();
    }
}

void T() {
    F();
    while (input[pos] == '*') {
        getToken();
        F();
    }
}

void F() {
    if (input[pos] == '(') {
        getToken();
        E();
        if (input[pos] == ')') {
            getToken();
        } else {
            flag = false;
        }
    } else if (input[pos] == 'i') {
        getToken();
    } else {
        flag = false;
    }
}

bool fourArithmeticOperationsParse(const char parseStr[]) {
    input = parseStr;
    pos = 0;
    flag = true;
    E();
    return flag && input[pos] == '\0';
}

int main() {
    const char* test1 = "i+i*(i+i)";
    const char* test2 = "(i+i)*i+i+i*i";

    if (fourArithmeticOperationsParse(test1)) {
        cout << "Test 1 success!" << endl;
    } else {
        cout << "Test 1 error!" << endl;
    }

    if (fourArithmeticOperationsParse(test2)) {
        cout << "Test 2 success!" << endl;
    } else {
        cout << "Test 2 error!" << endl;
    }

    return 0;
}