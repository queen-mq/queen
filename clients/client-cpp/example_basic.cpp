/**
 * Queen C++ Client - Basic Example
 *
 * Push five orders into one partition, consume them, and close:
 *
 *   make example && ./bin/example_basic [broker-url]
 */

#include "queen_client.hpp"

#include <iostream>
#include <string>

int main(int argc, char** argv) {
    const std::string url = argc > 1 ? argv[1] : "http://localhost:6632";

    try {
        queen::Queen queen(url);

        // One partition per entity: here, one customer.
        for (int i = 1; i <= 5; i++) {
            queen::PushItem order;
            order.payload = {{"order", i}, {"total", 10.0 * i}};
            order.transaction_id = "order-" + std::to_string(i);  // the dedup key
            auto results = queen.queue("cpp-example").partition("customer-42").push_items({order});
            std::cout << "order " << i << ": " << results[0].status_text << std::endl;
        }

        // Acked when the handler returns, retried if it throws.
        auto summary = queen.queue("cpp-example")
                           .limit(5)            // stop after five messages...
                           .idle_millis(3000)   // ...or three seconds without one
                           .consume([](const queen::Message& message) {
                               std::cout << "received " << message.data.dump() << std::endl;
                           });

        std::cout << "consumed " << summary.processed << " message(s), acked " << summary.acked
                  << std::endl;
        queen.close();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << std::endl;
        return 1;
    }
}
