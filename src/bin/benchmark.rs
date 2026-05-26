#![recursion_limit = "256"]
/// Benchmark LTX-2.3 generation across multiple ComfyUI worker instances.
///
/// Usage:
///   set -a && source .env && set +a && cargo run --bin benchmark
///
/// Outputs to ./benchmark_output/:
///   instance1_prompt01.mp4, instance2_prompt01.mp4, ...
///   results.csv  (instance, prompt_idx, status, gen_time_s, file)

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{fs, sync::Semaphore, task::JoinSet};
use uuid::Uuid;

// ── Config ─────────────────────────────────────────────────────────────────


const INSTANCES: &[(&str, &str)] = &[
    // (
    //     "instance1",
    //     "https://stolen-yours-renaissance-estimate.trycloudflare.com",
    // ),
    // (
    //     "instance2",
    //     "https://camping-liabilities-weights-supreme.trycloudflare.com",
    // ),
    (
        "instance3",
        "https://tries-stability-country-cabinets.trycloudflare.com",
    ),
    (
        "instance4",
        "https://bali-collar-few-friendly.trycloudflare.com",
    ),
    (
        "instance5",
        "https://provision-heavy-administration-currency.trycloudflare.com",
    ),
];

// Video duration in seconds (25 fps). 5 ≈ fast test | 30 ≈ full quality
const VIDEO_SECONDS: u32 = 15;

const POLL_INTERVAL: Duration = Duration::from_secs(10);
const TIMEOUT: Duration = Duration::from_secs(3 * 60 * 60); // 3 hours

const OUTPUT_DIR: &str = "benchmark_output";

const PROMPTS: &[&str] = &[
    r#"A girl walking through the streets of Tokyo at night eating street food, vlog style, neon lights everywhere."#,
    // r#"A medium shot opens on a sun-drenched patch of living room floor, where golden hour sunlight streams through a large window, illuminating dancing dust motes above a soft, worn rug. The air is warm and peaceful as Cooper, a fluffy cream-colored Shih Tzu wi"#,
    // r#"Medium shot of Raju "Rocket" Singh amidst the vibrant chaos of a bustling Indian street food stall at dusk. Flickering neon lights paint the wet pavement with electric hues as steam rises from sizzling woks, creating a thick, aromatic atmosphere. Raju, in"#,
    // r#"A medium shot opens on Dilkhush Rana Ji seated on a weathered wooden bench in a sun-dappled courtyard of a traditional Indian home. Golden hour sunlight filters through a neem tree, bathing the scene in a warm, comforting glow, highlighting the fine dust"#,
    // r#"Medium Shot: Ash Ketchum strides confidently along a sun-dappled forest path. Golden hour sunlight filters through the dense canopy, casting warm, shifting patterns on the rough tree trunks and the dusty ground, creating an atmosphere of vibrant anticipat"#,
    // r#"Medium shot of a vibrant, sun-drenched street market in Old Delhi. Golden hour sunlight bathes antique stalls, illuminating dust motes dancing above piles of richly embroidered textiles and intricate brassware, creating a warm, bustling atmosphere. Ashwri"#,
    // r#"Medium Close-up at Raju's Garam Snacks, a bustling street food stall in a vibrant Indian market. Warm, slightly hazy sunlight filters through a makeshift canopy, illuminating steam rising from a chai pot, and the air hums with the scent of spices and fryi"#,
    // r#"A medium shot opens inside Faruq's vibrant sari-sari store, packed with colorful goods. Warm, filtered sunlight streams through the open storefront, illuminating dust motes dancing above a worn wooden counter laden with snacks and essentials, creating a"#,
    // r#"A Medium shot opens on Priya, a vibrant Indian vlogger, amidst the bustling energy of a street food stall in a lively city market. Golden hour sunlight bathes the scene, creating a warm glow that catches the steam rising from a sizzling pan, highlighting"#,
    // r#"Medium Close-up on Raju behind his bustling chai tapri on a vibrant Indian street corner. Golden hour sunlight splashes across his worn wooden counter, illuminating the rising steam from bubbling chai pots and the vibrant chaos of the street. Raju, a lean"#,
];

// ── API types ───────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct GenerateRequest {
    input: GenerateInput,
}

#[derive(Serialize)]
struct GenerateInput {
    request_id: String,
    workflow_json: Value,
}

#[derive(Deserialize)]
struct GenerateResponse {
    id: String,
}

#[derive(Deserialize)]
struct JobStatus {
    status: String,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    output: Option<Vec<OutputFile>>,
    #[serde(default)]
    progress: Option<f64>,
}

#[derive(Deserialize)]
struct OutputFile {
    filename: String,
    #[serde(default)]
    subfolder: Option<String>,
}

// ── Workflow builder ────────────────────────────────────────────────────────

fn build_t2v_workflow(prompt: &str, _frames: u32) -> Value {
    // LTX-2.3 two-pass workflow (T2V, bypass=true so image input is ignored).
    // Matches exactly what off-chain-agent comfyui_client.rs sends.
    let mut w = serde_json::Map::new();

    // Model loaders
    w.insert("267:236".into(), json!({"inputs": {"ckpt_name": "ltx-2.3-22b-dev-fp8.safetensors"}, "class_type": "CheckpointLoaderSimple"}));
    w.insert("267:243".into(), json!({"inputs": {"text_encoder": "gemma_3_12B_it_fp4_mixed.safetensors", "ckpt_name": "ltx-2.3-22b-dev-fp8.safetensors", "device": "default"}, "class_type": "LTXAVTextEncoderLoader"}));
    w.insert("267:221".into(), json!({"inputs": {"ckpt_name": "ltx-2.3-22b-dev-fp8.safetensors"}, "class_type": "LTXVAudioVAELoader"}));
    w.insert("267:232".into(), json!({"inputs": {"lora_name": "ltx-2.3-22b-distilled-lora-384.safetensors", "strength_model": 0.5, "model": ["267:236", 0]}, "class_type": "LoraLoaderModelOnly"}));
    w.insert("267:233".into(), json!({"inputs": {"model_name": "ltx-2.3-spatial-upscaler-x2-1.1.safetensors"}, "class_type": "LatentUpscaleModelLoader"}));

    // Video parameters
    w.insert("267:201".into(), json!({"inputs": {"value": true}, "class_type": "PrimitiveBoolean"})); // is_t2v=true
    w.insert("267:260".into(), json!({"inputs": {"value": 25}, "class_type": "PrimitiveInt"}));       // fps
    w.insert("267:225".into(), json!({"inputs": {"value": VIDEO_SECONDS}, "class_type": "PrimitiveInt"})); // seconds
    w.insert("267:257".into(), json!({"inputs": {"value": 720}, "class_type": "PrimitiveInt"}));      // width
    w.insert("267:258".into(), json!({"inputs": {"value": 1280}, "class_type": "PrimitiveInt"}));     // height
    w.insert("267:261".into(), json!({"inputs": {"expression": "a", "values.a": ["267:260", 0]}, "class_type": "ComfyMathExpression"}));
    w.insert("267:277".into(), json!({"inputs": {"expression": "a * b + 1", "values.a": ["267:225", 0], "values.b": ["267:260", 0]}, "class_type": "ComfyMathExpression"}));
    w.insert("267:256".into(), json!({"inputs": {"expression": "a/2", "values.a": ["267:257", 0]}, "class_type": "ComfyMathExpression"}));
    w.insert("267:259".into(), json!({"inputs": {"expression": "a/2", "values.a": ["267:258", 0]}, "class_type": "ComfyMathExpression"}));

    // Prompts
    w.insert("267:266".into(), json!({"inputs": {"value": prompt}, "class_type": "PrimitiveStringMultiline"}));
    w.insert("267:240".into(), json!({"inputs": {"text": ["267:266", 0], "clip": ["267:243", 0]}, "class_type": "CLIPTextEncode"}));
    w.insert("267:247".into(), json!({"inputs": {"text": "pc game, console game, video game, cartoon, childish, ugly", "clip": ["267:243", 0]}, "class_type": "CLIPTextEncode"}));
    w.insert("267:239".into(), json!({"inputs": {"frame_rate": ["267:261", 0], "positive": ["267:240", 0], "negative": ["267:247", 0]}, "class_type": "LTXVConditioning"}));

    // Audio latent
    w.insert("267:214".into(), json!({"inputs": {"frames_number": ["267:277", 1], "frame_rate": ["267:261", 1], "batch_size": 1, "audio_vae": ["267:221", 0]}, "class_type": "LTXVEmptyLatentAudio"}));

    // Image input (bypassed for T2V)
    w.insert("267:276".into(), json!({"inputs": {"image": "example.png"}, "class_type": "LoadImage"}));
    w.insert("267:238".into(), json!({"inputs": {"resize_type": "scale dimensions", "resize_type.width": ["267:257", 0], "resize_type.height": ["267:258", 0], "resize_type.crop": "center", "scale_method": "lanczos", "input": ["267:276", 0]}, "class_type": "ResizeImageMaskNode"}));
    w.insert("267:235".into(), json!({"inputs": {"longer_edge": 1536, "images": ["267:238", 0]}, "class_type": "ResizeImagesByLongerEdge"}));
    w.insert("267:248".into(), json!({"inputs": {"img_compression": 18, "image": ["267:235", 0]}, "class_type": "LTXVPreprocess"}));

    // Pass 1: half-res latent
    w.insert("267:228".into(), json!({"inputs": {"width": ["267:256", 1], "height": ["267:259", 1], "length": ["267:277", 1], "batch_size": 1}, "class_type": "EmptyLTXVLatentVideo"}));
    w.insert("267:249".into(), json!({"inputs": {"strength": 0.7, "bypass": ["267:201", 0], "vae": ["267:236", 2], "image": ["267:248", 0], "latent": ["267:228", 0]}, "class_type": "LTXVImgToVideoInplace"}));
    w.insert("267:222".into(), json!({"inputs": {"video_latent": ["267:249", 0], "audio_latent": ["267:214", 0]}, "class_type": "LTXVConcatAVLatent"}));
    w.insert("267:237".into(), json!({"inputs": {"noise_seed": 0}, "class_type": "RandomNoise"}));
    w.insert("267:209".into(), json!({"inputs": {"sampler_name": "euler_ancestral_cfg_pp"}, "class_type": "KSamplerSelect"}));
    w.insert("267:252".into(), json!({"inputs": {"sigmas": "1.0, 0.99375, 0.9875, 0.98125, 0.975, 0.909375, 0.725, 0.421875, 0.0"}, "class_type": "ManualSigmas"}));
    w.insert("267:231".into(), json!({"inputs": {"cfg": 1, "model": ["267:232", 0], "positive": ["267:239", 0], "negative": ["267:239", 1]}, "class_type": "CFGGuider"}));
    w.insert("267:215".into(), json!({"inputs": {"noise": ["267:237", 0], "guider": ["267:231", 0], "sampler": ["267:209", 0], "sigmas": ["267:252", 0], "latent_image": ["267:222", 0]}, "class_type": "SamplerCustomAdvanced"}));
    w.insert("267:217".into(), json!({"inputs": {"av_latent": ["267:215", 0]}, "class_type": "LTXVSeparateAVLatent"}));

    // Upscale 2x
    w.insert("267:253".into(), json!({"inputs": {"samples": ["267:217", 0], "upscale_model": ["267:233", 0], "vae": ["267:236", 2]}, "class_type": "LTXVLatentUpsampler"}));

    // Pass 2: full-res refinement
    w.insert("267:230".into(), json!({"inputs": {"strength": 1.0, "bypass": ["267:201", 0], "vae": ["267:236", 2], "image": ["267:248", 0], "latent": ["267:253", 0]}, "class_type": "LTXVImgToVideoInplace"}));
    w.insert("267:229".into(), json!({"inputs": {"video_latent": ["267:230", 0], "audio_latent": ["267:217", 1]}, "class_type": "LTXVConcatAVLatent"}));
    w.insert("267:212".into(), json!({"inputs": {"positive": ["267:239", 0], "negative": ["267:239", 1], "latent": ["267:217", 0]}, "class_type": "LTXVCropGuides"}));
    w.insert("267:216".into(), json!({"inputs": {"noise_seed": 0}, "class_type": "RandomNoise"}));
    w.insert("267:246".into(), json!({"inputs": {"sampler_name": "euler_cfg_pp"}, "class_type": "KSamplerSelect"}));
    w.insert("267:211".into(), json!({"inputs": {"sigmas": "0.85, 0.7250, 0.4219, 0.0"}, "class_type": "ManualSigmas"}));
    w.insert("267:213".into(), json!({"inputs": {"cfg": 1, "model": ["267:232", 0], "positive": ["267:212", 0], "negative": ["267:212", 1]}, "class_type": "CFGGuider"}));
    w.insert("267:219".into(), json!({"inputs": {"noise": ["267:216", 0], "guider": ["267:213", 0], "sampler": ["267:246", 0], "sigmas": ["267:211", 0], "latent_image": ["267:229", 0]}, "class_type": "SamplerCustomAdvanced"}));
    w.insert("267:218".into(), json!({"inputs": {"av_latent": ["267:219", 0]}, "class_type": "LTXVSeparateAVLatent"}));

    // Decode + output
    w.insert("267:251".into(), json!({"inputs": {"tile_size": 768, "overlap": 64, "temporal_size": 4096, "temporal_overlap": 4, "samples": ["267:218", 0], "vae": ["267:236", 2]}, "class_type": "VAEDecodeTiled"}));
    w.insert("267:220".into(), json!({"inputs": {"samples": ["267:218", 1], "audio_vae": ["267:221", 0]}, "class_type": "LTXVAudioVAEDecode"}));
    w.insert("267:242".into(), json!({"inputs": {"fps": ["267:261", 0], "images": ["267:251", 0], "audio": ["267:220", 0]}, "class_type": "CreateVideo"}));
    w.insert("75".into(), json!({"inputs": {"filename_prefix": "video/ltx2-3", "format": "auto", "codec": "auto", "video-preview": "", "video": ["267:242", 0]}, "class_type": "SaveVideo"}));

    Value::Object(w)
}

// ── API helpers ─────────────────────────────────────────────────────────────

async fn submit_job(client: &Client, base_url: &str, token: &str, prompt: &str) -> Result<String> {
    let body = GenerateRequest {
        input: GenerateInput {
            request_id: Uuid::new_v4().to_string(),
            workflow_json: build_t2v_workflow(prompt, VIDEO_SECONDS),
        },
    };
    let resp = client
        .post(format!("{}/generate", base_url.trim_end_matches('/')))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {}/generate", base_url.trim_end_matches('/')))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("POST /generate {status}: {text}");
    }
    let gen: GenerateResponse = resp.json().await.context("parse generate response")?;
    Ok(gen.id)
}

async fn poll_until_done(
    client: &Client,
    base_url: &str,
    token: &str,
    job_id: &str,
    label: &str,
) -> Result<JobStatus> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if Instant::now() > deadline {
            anyhow::bail!("timeout after {}s", TIMEOUT.as_secs());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
        let resp = client
            .get(format!(
                "{}/result/{}",
                base_url.trim_end_matches('/'),
                job_id
            ))
            .bearer_auth(token)
            .send()
            .await
            .context("GET /result")?;

        if resp.status() == StatusCode::NOT_FOUND {
            continue;
        }
        if !resp.status().is_success() {
            let s = resp.status();
            let t = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET /result {s}: {t}");
        }
        let status: JobStatus = resp.json().await.context("parse job status")?;
        if let Some(p) = status.progress {
            println!("    [{label}] {} {p:.0}%", status.status);
        }
        match status.status.as_str() {
            "completed" | "failed" => return Ok(status),
            _ => {}
        }
    }
}

async fn download_video(
    client: &Client,
    base_url: &str,
    token: &str,
    file: &OutputFile,
    dest: &Path,
) -> Result<()> {
    let mut url = format!(
        "{}/view?filename={}",
        base_url.trim_end_matches('/'),
        file.filename
    );
    if let Some(sub) = &file.subfolder {
        if !sub.is_empty() {
            url.push_str(&format!("&subfolder={sub}"));
        }
    }
    let bytes = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .context("GET /view")?
        .error_for_status()
        .context("GET /view status")?
        .bytes()
        .await
        .context("read video bytes")?;
    fs::write(dest, &bytes).await.context("write video file")?;
    Ok(())
}

// ── Per-job runner ──────────────────────────────────────────────────────────

struct JobResult {
    instance: String,
    prompt_idx: usize,
    prompt_snippet: String,
    status: String,
    gen_time_s: f64,
    output_file: String,
    error: String,
}

async fn run_job(
    client: Arc<Client>,
    sem: Arc<Semaphore>,
    instance_name: String,
    base_url: String,
    token: String,
    prompt_idx: usize,
    prompt: String,
    output_dir: PathBuf,
) -> JobResult {
    let label = format!("{instance_name}_prompt{prompt_idx:02}");
    let snippet: String = prompt.chars().take(60).collect();

    // Wait for slot — ensures only 1 active job per instance at a time
    let _permit = sem.acquire().await.expect("semaphore closed");

    println!("  → [{label}] submitting…");
    let t0 = Instant::now();

    let result: Result<(String, String)> = async {
        let job_id = submit_job(&client, &base_url, &token, &prompt).await?;
        println!("  → [{label}] job={} polling…", &job_id[..8.min(job_id.len())]);

        let status = poll_until_done(&client, &base_url, &token, &job_id, &label).await?;

        if status.status == "completed" {
            if let Some(outputs) = &status.output {
                if let Some(first) = outputs.first() {
                    let dest = output_dir.join(format!("prompt{prompt_idx:02}.mp4"));
                    download_video(&client, &base_url, &token, first, &dest).await?;
                    return Ok((status.status, dest.to_string_lossy().into_owned()));
                }
            }
            Ok((status.status, String::new()))
        } else {
            Err(anyhow::anyhow!(
                "{}: {}",
                status.status,
                status.message.unwrap_or_default()
            ))
        }
    }
    .await;

    let gen_time_s = t0.elapsed().as_secs_f64();

    match result {
        Ok((status, path)) => {
            println!("  ✓ [{label}] {status} in {gen_time_s:.1}s → {path}");
            JobResult {
                instance: instance_name,
                prompt_idx,
                prompt_snippet: snippet,
                status,
                gen_time_s,
                output_file: path,
                error: String::new(),
            }
        }
        Err(e) => {
            println!("  ✗ [{label}] error: {e:#}");
            JobResult {
                instance: instance_name,
                prompt_idx,
                prompt_snippet: snippet,
                status: "error".into(),
                gen_time_s,
                output_file: String::new(),
                error: e.to_string(),
            }
        }
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let token = std::env::var("AUTH_TOKEN")
        .context("AUTH_TOKEN env var not set — set it or source your .env")?;
    let token = token.trim().to_string();

    let base_dir = PathBuf::from(OUTPUT_DIR);
    fs::create_dir_all(&base_dir).await?;

    // Find next try number
    let mut try_num = 1u32;
    while base_dir.join(format!("try{try_num}")).exists() {
        try_num += 1;
    }
    let output_dir = base_dir.join(format!("try{try_num}"));
    fs::create_dir_all(&output_dir).await?;
    println!("Run → {}\n", output_dir.display());

    let client = Arc::new(
        Client::builder()
            .timeout(Duration::from_secs(120))
            .danger_accept_invalid_certs(true)
            .build()?,
    );

    let total = INSTANCES.len() * PROMPTS.len();
    println!("Submitting {total} jobs ({} prompts × {} instances)…\n", PROMPTS.len(), INSTANCES.len());

    let mut set = JoinSet::new();
    for (name, url) in INSTANCES {
        let instance_dir = output_dir.join(name);
        fs::create_dir_all(&instance_dir).await?;
        // 1 permit = 1 active job at a time per instance (prevents queue timeouts)
        let sem = Arc::new(Semaphore::new(1));
        for (idx, prompt) in PROMPTS.iter().enumerate() {
            set.spawn(run_job(
                client.clone(),
                sem.clone(),
                name.to_string(),
                url.to_string(),
                token.clone(),
                idx + 1,
                prompt.to_string(),
                instance_dir.clone(),
            ));
        }
    }

    let mut results: Vec<JobResult> = Vec::with_capacity(total);
    while let Some(r) = set.join_next().await {
        results.push(r?);
    }

    results.sort_by_key(|r| (r.prompt_idx, r.instance.clone()));

    // CSV
    let csv_path = output_dir.join("results.csv");

    let mut csv = String::from("instance,prompt_idx,prompt_snippet,status,gen_time_s,output_file,error\n");
    for r in &results {
        let snippet = r.prompt_snippet.replace(',', ";");
        let err = r.error.replace(',', ";");
        csv.push_str(&format!(
            "{},{},{},{},{:.1},{},{}\n",
            r.instance, r.prompt_idx, snippet, r.status, r.gen_time_s, r.output_file, err
        ));
    }
    fs::write(&csv_path, csv).await?;

    // Summary table
    println!("\n{}", "─".repeat(88));
    println!("{:<14} {:>3} {:<12} {:>8}  {}", "Instance", "#", "Status", "Time", "File/Error");
    println!("{}", "─".repeat(88));
    for r in &results {
        let tail = if r.output_file.is_empty() {
            r.error.chars().take(45).collect::<String>()
        } else {
            r.output_file.clone()
        };
        println!(
            "{:<14} {:>3} {:<12} {:>7.1}s  {}",
            r.instance, r.prompt_idx, r.status, r.gen_time_s, tail
        );
    }
    println!("{}", "─".repeat(88));
    println!("Results → {}", csv_path.display());

    Ok(())
}
