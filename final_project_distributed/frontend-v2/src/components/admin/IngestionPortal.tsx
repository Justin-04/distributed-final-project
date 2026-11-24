import { useState, useCallback, useEffect, useRef } from 'react';
import { motion } from 'framer-motion';
import { UploadCloud, FileText, X, Loader2 } from 'lucide-react';
import { api, type IngestJob } from '@/services/api';
import { useAuth } from '@/hooks/use-auth';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Progress } from '@/components/ui/progress';
import { cn } from '@/lib/utils';

export default function IngestionPortal({ onIngested }: { onIngested: () => void }) {
  const { auth } = useAuth();
  const [file, setFile] = useState<File | null>(null);
  const [droneModel, setDroneModel] = useState('');
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [dragOver, setDragOver] = useState(false);
  const [job, setJob] = useState<IngestJob | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleFile = useCallback((f: File | null) => {
    if (f && f.type !== 'application/pdf' && !f.name.endsWith('.pdf')) {
      toast.error('Only PDF files are supported');
      return;
    }
    setFile(f);
  }, []);

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const f = e.dataTransfer.files[0];
    handleFile(f);
  };

  const handleUpload = async () => {
    if (!file) {
      toast.error('Please select a PDF file');
      return;
    }
    if (!droneModel.trim()) {
      toast.error('Please enter the drone model');
      return;
    }
    if (auth?.role !== 'admin') {
      toast.error('Admin access required');
      return;
    }

    setUploading(true);
    setProgress(5);
    setJob(null);

    try {
      const base64 = await fileToBase64(file);
      setProgress(10);
      // 202 Accepted: the document is queued; worker processes ingest it in parallel
      const { job_id } = await api.ingest(base64, droneModel.trim(), file.name);
      const final = await pollJob(job_id);
      setProgress(100);
      const secs = final.timings.elapsed_seconds?.toFixed(1) ?? '?';
      const nWorkers = Object.keys(final.workers).length;
      if (final.status === 'completed') {
        toast.success(`Ingested "${final.filename}" — ${final.chunks} chunks from ${final.total_pages} pages in ${secs}s (${nWorkers} worker${nWorkers === 1 ? '' : 's'})`);
      } else if (final.status === 'completed_with_errors') {
        toast.warning(`Ingested "${final.filename}" with ${final.pages_failed} failed page(s) — ${final.chunks} chunks in ${secs}s`);
      } else {
        toast.error(`Ingestion ${final.status}: ${final.error ?? 'see worker logs'}`);
      }
      setFile(null);
      setDroneModel('');
      onIngested();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Ingestion failed');
    } finally {
      setUploading(false);
      setTimeout(() => { setProgress(0); setJob(null); }, 4000);
    }
  };

  const pollJob = async (jobId: string): Promise<IngestJob> => {
    const terminal = ['completed', 'completed_with_errors', 'failed', 'cancelled'];
    for (;;) {
      const j = await api.getIngestJob(jobId);
      setJob(j);
      setProgress(10 + Math.round(j.progress * 90));
      if (terminal.includes(j.status)) return j;
      await new Promise((r) => setTimeout(r, 1500));
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className="glass rounded-2xl border border-cyan-500/15 p-6"
    >
      <div className="flex items-center gap-2 mb-4">
        <UploadCloud className="h-5 w-5 text-cyan-400" />
        <h3 className="font-semibold">Document Ingestion Portal</h3>
      </div>

      {/* Drop zone */}
      <div
        onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
        onClick={() => !uploading && inputRef.current?.click()}
        className={cn(
          'relative rounded-xl border-2 border-dashed p-8 text-center cursor-pointer transition-all',
          dragOver
            ? 'border-cyan-400/60 bg-cyan-500/5'
            : 'border-cyan-500/20 hover:border-cyan-500/40 hover:bg-white/[0.02]'
        )}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".pdf,application/pdf"
          className="hidden"
          onChange={(e) => handleFile(e.target.files?.[0] ?? null)}
        />
        {file ? (
          <div className="flex items-center justify-center gap-3">
            <FileText className="h-8 w-8 text-cyan-400" />
            <div className="text-left">
              <div className="text-sm font-medium">{file.name}</div>
              <div className="text-xs text-muted-foreground">{(file.size / 1024).toFixed(1)} KB</div>
            </div>
            {!uploading && (
              <button
                onClick={(e) => { e.stopPropagation(); setFile(null); }}
                className="ml-2 text-muted-foreground hover:text-rose-400 transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            )}
          </div>
        ) : (
          <div className="space-y-2">
            <UploadCloud className="h-10 w-10 text-cyan-400/50 mx-auto" />
            <div className="text-sm text-muted-foreground">
              Drag & drop a <span className="text-cyan-300">PDF</span> file here, or click to browse
            </div>
          </div>
        )}
      </div>

      {/* Drone model input */}
      <div className="mt-4 space-y-2">
        <label className="text-xs font-mono text-muted-foreground">DRONE MODEL</label>
        <Input
          value={droneModel}
          onChange={(e) => setDroneModel(e.target.value)}
          placeholder="e.g. DJI Mavic 3 Enterprise"
          className="bg-[#0B0F19]/60 border-cyan-500/20 focus-visible:ring-cyan-500/40"
        />
      </div>

      {/* Progress */}
      {(uploading || job) && (
        <div className="mt-4 space-y-1.5">
          <Progress value={progress} className="h-1.5 bg-cyan-500/10" />
          <div className="text-xs font-mono text-cyan-300/70">
            {!job
              ? 'Uploading PDF...'
              : job.status === 'queued' || job.status === 'splitting'
                ? `Job ${job.job_id} ${job.status}...`
                : `${job.status === 'processing' ? 'Processing' : job.status} · page ${job.pages_finished}/${job.total_pages} · ${Object.keys(job.workers).length} worker(s) · ${job.timings.elapsed_seconds?.toFixed(0) ?? 0}s`}
          </div>
          {job && job.chunks > 0 && (
            <div className="text-[11px] font-mono text-muted-foreground">
              {job.chunks} chunks · {job.images} images captioned{job.pages_failed > 0 ? ` · ${job.pages_failed} failed` : ''}
            </div>
          )}
        </div>
      )}

      {/* Submit */}
      <Button
        onClick={handleUpload}
        disabled={uploading || !file || !droneModel.trim()}
        className="w-full mt-4 bg-gradient-to-r from-cyan-500 to-blue-600 hover:from-cyan-400 hover:to-blue-500 text-white border-0"
      >
        {uploading ? (
          <>
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            Ingesting...
          </>
        ) : (
          <>
            <UploadCloud className="h-4 w-4 mr-2" />
            Ingest Document
          </>
        )}
      </Button>
    </motion.div>
  );
}

function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = reader.result as string;
      const base64 = result.split(',')[1] ?? result;
      resolve(base64);
    };
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}
