// future::wait
#include <iostream>       // std::cout
#include <future>         // std::async, std::future
#include <chrono>         // std::chrono::milliseconds

// a non-optimized way of checking for prime numbers:
bool is_prime (int x) {
  for (int i=2; i<x; ++i)  {
      if (x%i==0) 
          return false;
      if (i > 1000000)
          throw 5;
  }
  return true;
}

int main ()
{
  // call function asynchronously:
  std::future<bool> fut = std::async (is_prime,194232491); 

  std::cout << "checking...\n";
  try {
      fut.wait();
  } catch (...) {
      std::cout << "exception in wait()" << std::endl;
  }

  try {
      std::cout << "\n194232491 ";
      if (fut.get())      // guaranteed to be ready (and not block) after wait returns
        std::cout << "is prime.\n";
      else
        std::cout << "is not prime.\n";
  } catch (...) {
      std::cout << "exception in get()" << std::endl;
  }

  return 0;
}
