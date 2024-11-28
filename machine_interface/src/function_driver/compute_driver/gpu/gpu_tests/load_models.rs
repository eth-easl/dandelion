use crate::{
    function_driver::compute_driver::gpu::gpu_tests::load_utils::{
        add_buffer, add_empty_buffer, add_number, read_tensor_from_file,
    },
    memory_domain::Context,
};

pub fn load_double_matmul(mut function_context: Context) -> (usize, String, Vec<f32>, Context) {
    let constants_path = "/home/alrusso/pytorch-aot/processed/double_matmul/constants";

    add_buffer("arg2_1", 32, constants_path, &mut function_context);
    add_buffer("linear1_weight", 40, constants_path, &mut function_context);
    add_buffer("linear2_weight", 60, constants_path, &mut function_context);

    let output_name: &str = "buf1";
    let output_size: usize = 48;
    let expected: Vec<f32> = read_tensor_from_file(output_name, constants_path).unwrap();

    (output_size, output_name.to_string(), expected, function_context)
}

pub fn load_resnet18(mut function_context: Context) -> (usize, String, Vec<f32>, Context) {
    let constants_path = "/home/alrusso/pytorch-aot/processed/resnet18/constants";

    add_buffer("arg122_1", 602112, constants_path, &mut function_context);
    add_buffer("conv1_weight", 37632, constants_path, &mut function_context);
    add_buffer("bn1_running_mean", 256, constants_path, &mut function_context);
    add_buffer("bn1_running_var", 256, constants_path, &mut function_context);
    add_buffer("bn1_weight", 256, constants_path, &mut function_context);
    add_buffer("bn1_bias", 256, constants_path, &mut function_context);
    add_number("var_9", 200704, &mut function_context);
    add_buffer("layer1_0_conv1_weight", 147456, constants_path, &mut function_context);
    add_buffer("layer1_0_bn1_running_mean", 256, constants_path, &mut function_context);
    add_buffer("layer1_0_bn1_running_var", 256, constants_path, &mut function_context);
    add_buffer("layer1_0_bn1_weight", 256, constants_path, &mut function_context);
    add_buffer("layer1_0_bn1_bias", 256, constants_path, &mut function_context);
    add_buffer("layer1_0_conv2_weight", 147456, constants_path, &mut function_context);
    add_buffer("layer1_0_bn2_running_mean", 256, constants_path, &mut function_context);
    add_buffer("layer1_0_bn2_running_var", 256, constants_path, &mut function_context);
    add_buffer("layer1_0_bn2_weight", 256, constants_path, &mut function_context);
    add_buffer("layer1_0_bn2_bias", 256, constants_path, &mut function_context);
    add_buffer("layer1_1_conv1_weight", 147456, constants_path, &mut function_context);
    add_buffer("layer1_1_bn1_running_mean", 256, constants_path, &mut function_context);
    add_buffer("layer1_1_bn1_running_var", 256, constants_path, &mut function_context);
    add_buffer("layer1_1_bn1_weight", 256, constants_path, &mut function_context);
    add_buffer("layer1_1_bn1_bias", 256, constants_path, &mut function_context);
    add_buffer("layer1_1_conv2_weight", 147456, constants_path, &mut function_context);
    add_buffer("layer1_1_bn2_running_mean", 256, constants_path, &mut function_context);
    add_buffer("layer1_1_bn2_running_var", 256, constants_path, &mut function_context);
    add_buffer("layer1_1_bn2_weight", 256, constants_path, &mut function_context);
    add_buffer("layer1_1_bn2_bias", 256, constants_path, &mut function_context);
    add_buffer("layer2_0_conv1_weight", 294912, constants_path, &mut function_context);
    add_buffer("layer2_0_bn1_running_mean", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_bn1_running_var", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_bn1_weight", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_bn1_bias", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_conv2_weight", 589824, constants_path, &mut function_context);
    add_buffer("layer2_0_downsample_0_weight", 32768, constants_path, &mut function_context);
    add_buffer("layer2_0_bn2_running_mean", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_bn2_running_var", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_bn2_weight", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_bn2_bias", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_downsample_1_running_mean", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_downsample_1_running_var", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_downsample_1_weight", 512, constants_path, &mut function_context);
    add_buffer("layer2_0_downsample_1_bias", 512, constants_path, &mut function_context);
    add_number("var_63", 100352, &mut function_context);
    add_buffer("layer2_1_conv1_weight", 589824, constants_path, &mut function_context);
    add_buffer("layer2_1_bn1_running_mean", 512, constants_path, &mut function_context);
    add_buffer("layer2_1_bn1_running_var", 512, constants_path, &mut function_context);
    add_buffer("layer2_1_bn1_weight", 512, constants_path, &mut function_context);
    add_buffer("layer2_1_bn1_bias", 512, constants_path, &mut function_context);
    add_buffer("layer2_1_conv2_weight", 589824, constants_path, &mut function_context);
    add_buffer("layer2_1_bn2_running_mean", 512, constants_path, &mut function_context);
    add_buffer("layer2_1_bn2_running_var", 512, constants_path, &mut function_context);
    add_buffer("layer2_1_bn2_weight", 512, constants_path, &mut function_context);
    add_buffer("layer2_1_bn2_bias", 512, constants_path, &mut function_context);
    add_buffer("layer3_0_conv1_weight", 1179648, constants_path, &mut function_context);
    add_buffer("layer3_0_bn1_running_mean", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_bn1_running_var", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_bn1_weight", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_bn1_bias", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_conv2_weight", 2359296, constants_path, &mut function_context);
    add_buffer("layer3_0_downsample_0_weight", 131072, constants_path, &mut function_context);
    add_buffer("layer3_0_bn2_running_mean", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_bn2_running_var", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_bn2_weight", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_bn2_bias", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_downsample_1_running_mean", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_downsample_1_running_var", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_downsample_1_weight", 1024, constants_path, &mut function_context);
    add_buffer("layer3_0_downsample_1_bias", 1024, constants_path, &mut function_context);
    add_buffer("layer3_1_conv1_weight", 2359296, constants_path, &mut function_context);
    add_buffer("layer3_1_bn1_running_mean", 1024, constants_path, &mut function_context);
    add_buffer("layer3_1_bn1_running_var", 1024, constants_path, &mut function_context);
    add_buffer("layer3_1_bn1_weight", 1024, constants_path, &mut function_context);
    add_buffer("layer3_1_bn1_bias", 1024, constants_path, &mut function_context);
    add_number("var_109", 50176, &mut function_context);
    add_buffer("layer3_1_conv2_weight", 2359296, constants_path, &mut function_context);
    add_buffer("layer3_1_bn2_running_mean", 1024, constants_path, &mut function_context);
    add_buffer("layer3_1_bn2_running_var", 1024, constants_path, &mut function_context);
    add_buffer("layer3_1_bn2_weight", 1024, constants_path, &mut function_context);
    add_buffer("layer3_1_bn2_bias", 1024, constants_path, &mut function_context);
    add_number("var_119", 50176, &mut function_context);
    add_buffer("layer4_0_conv1_weight", 4718592, constants_path, &mut function_context);
    add_buffer("layer4_0_bn1_running_mean", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_bn1_running_var", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_bn1_weight", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_bn1_bias", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_conv2_weight", 9437184, constants_path, &mut function_context);
    add_buffer("layer4_0_downsample_0_weight", 524288, constants_path, &mut function_context);
    add_buffer("layer4_0_bn2_running_mean", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_bn2_running_var", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_bn2_weight", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_bn2_bias", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_downsample_1_running_mean", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_downsample_1_running_var", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_downsample_1_weight", 2048, constants_path, &mut function_context);
    add_buffer("layer4_0_downsample_1_bias", 2048, constants_path, &mut function_context);
    add_number("var_143", 25088, &mut function_context);
    add_buffer("layer4_1_conv1_weight", 9437184, constants_path, &mut function_context);
    add_buffer("layer4_1_bn1_running_mean", 2048, constants_path, &mut function_context);
    add_buffer("layer4_1_bn1_running_var", 2048, constants_path, &mut function_context);
    add_buffer("layer4_1_bn1_weight", 2048, constants_path, &mut function_context);
    add_buffer("layer4_1_bn1_bias", 2048, constants_path, &mut function_context);
    add_buffer("layer4_1_conv2_weight", 9437184, constants_path, &mut function_context);
    add_buffer("layer4_1_bn2_running_mean", 2048, constants_path, &mut function_context);
    add_buffer("layer4_1_bn2_running_var", 2048, constants_path, &mut function_context);
    add_buffer("layer4_1_bn2_weight", 2048, constants_path, &mut function_context);
    add_buffer("layer4_1_bn2_bias", 2048, constants_path, &mut function_context);
    add_number("var_161", 512, &mut function_context);
    add_number("var_162", 49, &mut function_context);
    add_buffer("fc_bias", 4000, constants_path, &mut function_context);
    add_buffer("fc_weight", 2048000, constants_path, &mut function_context);

    let output_name: &str = "buf42";
    let output_size: usize = 4000;
    let expected: Vec<f32> = read_tensor_from_file(output_name, constants_path).unwrap();

    (output_size, output_name.to_string(), expected, function_context)
}

pub fn load_alexnet(mut function_context: Context) -> (usize, String, Vec<f32>, Context) {
    let constants_path = "/home/alrusso/pytorch-aot/processed/alexnet/constants";

    add_buffer("arg16_1", 618348, constants_path, &mut function_context);
    add_buffer("features_0_weight", 92928, constants_path, &mut function_context);
    add_buffer("features_0_bias", 256, constants_path, &mut function_context);
    add_number("var_6", 46656, &mut function_context);
    add_buffer("features_3_weight", 1228800, constants_path, &mut function_context);
    add_buffer("features_3_bias", 768, constants_path, &mut function_context);
    add_number("var_13", 32448, &mut function_context);
    add_buffer("features_6_weight", 2654208, constants_path, &mut function_context);
    add_buffer("features_6_bias", 1536, constants_path, &mut function_context);
    add_buffer("features_8_weight", 3538944, constants_path, &mut function_context);
    add_buffer("features_8_bias", 1024, constants_path, &mut function_context);
    add_buffer("features_10_weight", 2359296, constants_path, &mut function_context);
    add_buffer("features_10_bias", 1024, constants_path, &mut function_context);
    add_number("var_28", 9216, &mut function_context);
    add_buffer("classifier_1_weight", 150994944, constants_path, &mut function_context);
    add_buffer("classifier_1_bias", 16384, constants_path, &mut function_context);
    add_buffer("classifier_4_weight", 67108864, constants_path, &mut function_context);
    add_buffer("classifier_4_bias", 16384, constants_path, &mut function_context);
    add_buffer("classifier_6_bias", 40, constants_path, &mut function_context);
    add_buffer("classifier_6_weight", 163840, constants_path, &mut function_context);

    let output_name: &str = "buf18";
    let output_size: usize = 40;
    let expected: Vec<f32> = read_tensor_from_file(output_name, constants_path).unwrap();

    (output_size, output_name.to_string(), expected, function_context)
}

pub fn load_lenet5(mut function_context: Context) -> (usize, String, Vec<f32>, Context) {
    let constants_path = "/home/alrusso/pytorch-aot/processed/lenet5/constants";

    add_number("var_1", 6, &mut function_context);
    add_buffer("arg10_1", 3136, constants_path, &mut function_context);
    add_buffer("conv1_weight", 600, constants_path, &mut function_context);
    add_number("var_8", 1176, &mut function_context);
    add_buffer("conv2_weight", 9600, constants_path, &mut function_context);
    add_buffer("conv2_bias", 64, constants_path, &mut function_context);
    add_number("var_15", 400, &mut function_context);
    add_buffer("fc1_weight", 192000, constants_path, &mut function_context);
    add_buffer("fc1_bias", 480, constants_path, &mut function_context);
    add_buffer("fc2_weight", 40320, constants_path, &mut function_context);
    add_buffer("fc2_bias", 336, constants_path, &mut function_context);
    add_buffer("fc3_bias", 40, constants_path, &mut function_context);
    add_buffer("fc3_weight", 3360, constants_path, &mut function_context);

    let output_name: &str = "buf11";
    let output_size: usize = 40;
    let expected: Vec<f32> = read_tensor_from_file(output_name, constants_path).unwrap();

    (output_size, output_name.to_string(), expected, function_context)
}

pub fn load_batch_norm(mut function_context: Context) -> (usize, String, Vec<f32>, Context) {
    let constants_path = "/home/alrusso/pytorch-aot/processed/batch_norm/constants";

    add_buffer("arg9_1", 6400, constants_path, &mut function_context);
    add_buffer("conv_weight", 147456, constants_path, &mut function_context);
    add_buffer("conv_bias", 256, constants_path, &mut function_context);
    add_buffer("bn_running_mean", 256, constants_path, &mut function_context);
    add_buffer("bn_running_var", 256, constants_path, &mut function_context);
    add_buffer("bn_weight", 256, constants_path, &mut function_context);
    add_buffer("bn_bias", 256, constants_path, &mut function_context);
    add_number("var_10", 256, &mut function_context);
    add_number("var_12", 3, &mut function_context);
    add_buffer("fc_weight", 3072, constants_path, &mut function_context);

    let output_name: &str = "buf5";
    let output_size: usize = 12;
    let expected: Vec<f32> = read_tensor_from_file(output_name, constants_path).unwrap();

    (output_size, output_name.to_string(), expected, function_context)
}
