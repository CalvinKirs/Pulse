#include <azure/core/diagnostics/logger.hpp>
#include <azure/core/http/http_status_code.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/common/storage_credential.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

namespace {

struct Options {
    std::string account_name;
    std::string account_key;
    std::string container;
    std::string endpoint {"blob.core.windows.net"};
    std::string container_url;
    std::string mode {"basic"};
    std::string prefix {"doris-connectivity-check/"};
    std::string ca_info;
    std::string ca_path;
    int log_level {3};
};

std::string to_lower(std::string s) {
    for (char& c : s) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c - 'A' + 'a');
        }
    }
    return s;
}

bool starts_with(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

std::string trim_trailing_slash(std::string s) {
    while (!s.empty() && s.back() == '/') {
        s.pop_back();
    }
    return s;
}

std::string normalize_endpoint(std::string endpoint) {
    endpoint = trim_trailing_slash(endpoint);
    if (starts_with(endpoint, "https://")) {
        endpoint = endpoint.substr(8);
    } else if (starts_with(endpoint, "http://")) {
        endpoint = endpoint.substr(7);
    }
    return endpoint;
}

std::string normalize_prefix(std::string prefix) {
    if (prefix.empty()) {
        return prefix;
    }
    if (prefix.back() != '/') {
        prefix.push_back('/');
    }
    return prefix;
}

std::string make_container_url(const Options& opts) {
    if (!opts.container_url.empty()) {
        return trim_trailing_slash(opts.container_url);
    }
    std::string endpoint = normalize_endpoint(opts.endpoint);
    return "https://" + opts.account_name + "." + endpoint + "/" + opts.container;
}

std::string make_temp_blob_name(const std::string& prefix) {
    const auto now = std::chrono::system_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    return normalize_prefix(prefix) + "azure-connectivity-" + std::to_string(ms);
}

void print_usage(const char* prog) {
    std::cout << "Azure Blob connectivity client\n\n"
              << "Required:\n"
              << "  --account-name <name>\n"
              << "  --account-key <key>\n"
              << "  --container <container> (unless --container-url is used)\n\n"
              << "Optional:\n"
              << "  --endpoint <host>            default: blob.core.windows.net\n"
              << "  --container-url <full_url>   e.g. https://acct.blob.core.windows.net/container\n"
              << "  --mode <basic|rw>            default: basic\n"
              << "  --prefix <blob_prefix>       default: doris-connectivity-check/\n"
              << "  --ca-info <pem_file>         optional curl CA bundle file path\n"
              << "  --ca-path <pem_dir>          optional curl CA cert directory\n"
              << "  --log-level <0-4>            Azure SDK log level, default: 3\n\n"
              << "Examples:\n"
              << "  " << prog
              << " --account-name myacct --account-key '***' --container mycontainer --mode basic\n"
              << "  " << prog
              << " --account-name myacct --account-key '***' --container mycontainer --mode rw --prefix test/\n";
}

bool parse_args(int argc, char** argv, Options* opts, std::string* err) {
    std::unordered_map<std::string, std::string> args;
    for (int i = 1; i < argc; ++i) {
        std::string cur = argv[i];
        if (cur == "--help" || cur == "-h") {
            args["help"] = "1";
            continue;
        }
        if (!starts_with(cur, "--")) {
            *err = "Unexpected argument: " + cur;
            return false;
        }
        std::string key;
        std::string value;
        auto eq_pos = cur.find('=');
        if (eq_pos != std::string::npos) {
            key = cur.substr(2, eq_pos - 2);
            value = cur.substr(eq_pos + 1);
        } else {
            key = cur.substr(2);
            if (i + 1 >= argc || starts_with(argv[i + 1], "--")) {
                *err = "Missing value for --" + key;
                return false;
            }
            value = argv[++i];
        }
        args[key] = value;
    }

    if (args.find("help") != args.end()) {
        return true;
    }

    if (args.find("account-name") != args.end()) {
        opts->account_name = args["account-name"];
    }
    if (args.find("account-key") != args.end()) {
        opts->account_key = args["account-key"];
    }
    if (args.find("container") != args.end()) {
        opts->container = args["container"];
    }
    if (args.find("endpoint") != args.end()) {
        opts->endpoint = args["endpoint"];
    }
    if (args.find("container-url") != args.end()) {
        opts->container_url = args["container-url"];
    }
    if (args.find("mode") != args.end()) {
        opts->mode = to_lower(args["mode"]);
    }
    if (args.find("prefix") != args.end()) {
        opts->prefix = args["prefix"];
    }
    if (args.find("ca-info") != args.end()) {
        opts->ca_info = args["ca-info"];
    }
    if (args.find("ca-path") != args.end()) {
        opts->ca_path = args["ca-path"];
    }
    if (args.find("log-level") != args.end()) {
        try {
            opts->log_level = std::stoi(args["log-level"]);
        } catch (...) {
            *err = "Invalid --log-level: " + args["log-level"];
            return false;
        }
    }

    if (opts->account_name.empty()) {
        *err = "Missing required --account-name";
        return false;
    }
    if (opts->account_key.empty()) {
        *err = "Missing required --account-key";
        return false;
    }
    if (opts->container_url.empty() && opts->container.empty()) {
        *err = "Missing required --container (or provide --container-url)";
        return false;
    }
    if (opts->mode != "basic" && opts->mode != "rw") {
        *err = "Invalid --mode: " + opts->mode + " (expected basic or rw)";
        return false;
    }
    if (opts->log_level < 0 || opts->log_level > 4) {
        *err = "Invalid --log-level: expected 0..4";
        return false;
    }
    return true;
}

int run_basic(const Azure::Storage::Blobs::BlobContainerClient& container_client,
              const std::string& container_url) {
    auto resp = container_client.GetProperties();
    std::cout << "[PASS] container reachable\n"
              << "  url: " << container_url << "\n"
              << "  etag: " << resp.Value.ETag.ToString() << "\n";
    return 0;
}

int run_rw(const Azure::Storage::Blobs::BlobContainerClient& container_client,
           const std::string& container_url, const std::string& prefix) {
    const std::string blob_name = make_temp_blob_name(prefix);
    const std::string payload = "azure-connectivity-payload:" + blob_name;

    auto blob_client = container_client.GetBlockBlobClient(blob_name);
    bool uploaded = false;

    try {
        blob_client.UploadFrom(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
        uploaded = true;

        auto prop = blob_client.GetProperties();
        if (prop.Value.BlobSize != static_cast<int64_t>(payload.size())) {
            std::cerr << "[FAIL] uploaded blob size mismatch, expected=" << payload.size()
                      << ", actual=" << prop.Value.BlobSize << "\n";
            try {
                blob_client.Delete();
            } catch (...) {
            }
            return 1;
        }

        blob_client.Delete();
        std::cout << "[PASS] read/write/delete succeeded\n"
                  << "  container: " << container_url << "\n"
                  << "  blob: " << blob_name << "\n";
        return 0;
    } catch (...) {
        if (uploaded) {
            try {
                blob_client.Delete();
            } catch (...) {
            }
        }
        throw;
    }
}

} // namespace

int main(int argc, char** argv) {
    Options opts;
    std::string err;
    if (!parse_args(argc, argv, &opts, &err)) {
        std::cerr << "Argument error: " << err << "\n\n";
        print_usage(argv[0]);
        return 2;
    }

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    try {
        auto azure_level = static_cast<Azure::Core::Diagnostics::Logger::Level>(opts.log_level);
        Azure::Core::Diagnostics::Logger::SetLevel(azure_level);

        const std::string container_url = make_container_url(opts);
        auto credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
                opts.account_name, opts.account_key);
        Azure::Storage::Blobs::BlobClientOptions client_options;
        client_options.Retry.StatusCodes.insert(Azure::Core::Http::HttpStatusCode::TooManyRequests);
        if (!opts.ca_info.empty() || !opts.ca_path.empty()) {
            Azure::Core::Http::CurlTransportOptions curl_options;
            if (!opts.ca_info.empty()) {
                curl_options.CAInfo = opts.ca_info;
            }
            if (!opts.ca_path.empty()) {
                curl_options.CAPath = opts.ca_path;
            }
            client_options.Transport.Transport =
                    std::make_shared<Azure::Core::Http::CurlTransport>(curl_options);
        }

        Azure::Storage::Blobs::BlobContainerClient container_client(container_url, credential,
                                                                    client_options);

        if (opts.mode == "rw") {
            return run_rw(container_client, container_url, opts.prefix);
        }
        return run_basic(container_client, container_url);
    } catch (const Azure::Core::RequestFailedException& ex) {
        std::cerr << "[FAIL] Azure request failed\n"
                  << "  what: " << ex.what() << "\n"
                  << "  message: " << ex.Message << "\n"
                  << "  status_code: " << static_cast<int>(ex.StatusCode) << "\n"
                  << "  error_code: " << ex.ErrorCode << "\n"
                  << "  request_id: " << ex.RequestId << "\n";
        return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] Unexpected error: " << ex.what() << "\n";
        return 1;
    }
}
