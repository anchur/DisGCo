#ifndef GM_ATOMIC_OPERATIONS_H_
#define GM_ATOMIC_OPERATIONS_H_
//mpi
#include <mpi.h>
//mpi

#include <typeinfo>

template<typename T>
MPI_Datatype get_type()
{
    char name = typeid(T).name()[0];
    switch (name) {
        case 'i':
            return MPI_INT;
            break;
        case 'f':
            return MPI_FLOAT;
            break;
        case 'j':
            return MPI_UNSIGNED;
            break;
        case 'd':
            return MPI_DOUBLE;
            break;
        case 'c':
            return MPI_CHAR;
            break;
        case 's':
            return MPI_SHORT;
            break;
        case 'l':
            return MPI_LONG;
            break;
        case 'm':
            return MPI_UNSIGNED_LONG;
            break;
        case 'b':
            return MPI_BYTE;
            break;
    }
}

template<typename T>
inline void ATOMIC_ADD(T* target, T value) {
    if (value == 0) return;

    T oldValue, newValue;
    do {
        oldValue = *target;
        newValue = oldValue + value;
    } while (_gm_atomic_compare_and_swap(target, oldValue, newValue) == false);

}


template<typename T>
inline void MPI_ADD(T* target, T value) {
	//mpi

	MPI_Datatype type = get_type<T>();
	MPI_Allreduce(&value, target, 1, type, MPI_SUM, MPI_COMM_WORLD);
	//return (*target + value) ;
	//mpi
}



template<typename T>
inline void ATOMIC_MULT(T* target, T value) {

    if (value == 1) return;

    T oldValue, newValue;
    do {
        oldValue = *target;
        newValue = oldValue * value;
    } while (_gm_atomic_compare_and_swap(target, oldValue, newValue) == false);
}

template<typename T>
inline void MPI_MULT(T* target, T value) {
	//mpi

	MPI_Datatype type = get_type<T>();
	MPI_Allreduce(&value, target, 1, type, MPI_PROD, MPI_COMM_WORLD);
	//return (*target + value) ;
	//mpi
}


template<typename T>
inline void ATOMIC_MIN(T* target, T value) {
    T oldValue, newValue;
    do {
        oldValue = *target;
        newValue = std::min(oldValue, value);
        if (oldValue == newValue) return;
    } while (_gm_atomic_compare_and_swap(target, oldValue, newValue) == false);
}

template<typename T>
inline void ATOMIC_MAX(T* target, T value) {
    T oldValue, newValue;
    do {
        oldValue = *target;
        newValue = std::max(oldValue, value);
        if (oldValue == newValue) return;
    } while (_gm_atomic_compare_and_swap(target, oldValue, newValue) == false);
}


inline void ATOMIC_AND(bool* target, bool new_value)
{
    // if new value is true, AND operation does not make a change
    // if old target value is false, AND operation does not make a change
    if ((new_value == false) && (*target == true)) *target = false;
}
inline void MPI_AND(bool* target, bool new_value)
{
    // if new value is true, AND operation does not make a change
    // if old target value is false, AND operation does not make a change
    //if ((new_value == false) && (*target == true)) *target = false;
    //mpi
	MPI_Allreduce(&new_value, target, 1, MPI_C_BOOL, MPI_LAND, MPI_COMM_WORLD);
	//MPI_Allreduce(&new_value, target, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	//mpi
}


inline void ATOMIC_OR(bool* target, bool new_value)
{
    // if new value is false, OR operation does not make a change
    // if old target value is true, OR operation does not make a change
    if ((new_value == true) && (*target == false)) *target = true;
    //mpi
	//MPI_Allreduce(&new_value, target, 1, MPI_C_BOOL, MPI_LOR, MPI_COMM_WORLD);
	//mpi
}

inline void MPI_OR(bool* target, bool new_value)
{
    // if new value is false, OR operation does not make a change
    // if old target value is true, OR operation does not make a change
    //if ((new_value == true) && (*target == false)) *target = true;
    //mpi
	MPI_Allreduce(&new_value, target, 1, MPI_C_BOOL, MPI_LOR, MPI_COMM_WORLD);
	//MPI_Allreduce(&new_value, target, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
	//mpi
}


#endif /* GM_ATOMIC_OPERATIONS_H_ */
