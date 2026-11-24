import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui/card';
import { Button } from '../ui/button';
import { Badge } from '../ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../ui/tabs';
import { 
  Activity, 
  DollarSign, 
  Zap, 
  TrendingUp, 
  Clock, 
  RefreshCw,
  ChevronDown,
  ChevronRight,
  Terminal,
  Cpu,
  Database
} from 'lucide-react';
import { api } from '../../services/api';

interface Trace {
  id: string;
  timestamp: string;
  name: string;
  user_id: string;
  metadata: {
    route?: string;
    intent?: string;
    [key: string]: any;
  };
  input: any;
  output: any;
  duration_ms: number;
  total_cost: number;
  total_tokens: number;
  route: string;
  intent: string;
}

interface TraceDetails {
  trace: {
    id: string;
    timestamp: string;
    name: string;
    user_id: string;
    input: any;
    output: any;
    metadata: any;
    duration_ms: number;
    total_cost: number;
    total_tokens: number;
  };
  tool_calls: Array<{
    id: string;
    name: string;
    start_time: string;
    end_time: string;
    duration_ms: number;
    input: any;
    output: any;
    metadata: any;
    status: string;
  }>;
  llm_generations: Array<{
    id: string;
    name: string;
    model: string;
    start_time: string;
    end_time: string;
    duration_ms: number;
    input: any;
    output: any;
    tokens: number;
    cost: number;
  }>;
  spans: Array<{
    id: string;
    name: string;
    start_time: string;
    end_time: string;
    duration_ms: number;
    input: any;
    output: any;
  }>;
}

interface UsageStats {
  period_days: number;
  from: string;
  to: string;
  total_requests: number;
  total_tokens: number;
  total_cost: number;
  avg_latency_ms: number;
  requests_by_route: Record<string, number>;
  tokens_by_model: Record<string, number>;
}

const TelemetryDashboard: React.FC = () => {
  const [traces, setTraces] = useState<Trace[]>([]);
  const [stats, setStats] = useState<UsageStats | null>(null);
  const [selectedTrace, setSelectedTrace] = useState<TraceDetails | null>(null);
  const [loading, setLoading] = useState(false);
  const [expandedTraceId, setExpandedTraceId] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);

  const loadTraces = async (pageNum: number = 1) => {
    setLoading(true);
    try {
      const response = await api.getTelemetryTraces(pageNum, 20);
      setTraces(response.traces || []);
      setTotalPages(response.pagination?.totalPages || 1);
      setPage(pageNum);
    } catch (error) {
      console.error('Failed to load traces:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadStats = async () => {
    try {
      const data = await api.getTelemetryStats(7);
      setStats(data);
    } catch (error) {
      console.error('Failed to load stats:', error);
    }
  };

  const loadTraceDetails = async (traceId: string) => {
    if (expandedTraceId === traceId) {
      setExpandedTraceId(null);
      setSelectedTrace(null);
      return;
    }

    try {
      const details = await api.getTelemetryTraceDetails(traceId);
      setSelectedTrace(details);
      setExpandedTraceId(traceId);
    } catch (error) {
      console.error('Failed to load trace details:', error);
    }
  };

  useEffect(() => {
    loadTraces();
    loadStats();
  }, []);

  const formatCost = (cost: number) => {
    return `$${cost.toFixed(4)}`;
  };

  const formatDuration = (ms: number) => {
    if (ms < 1000) return `${ms.toFixed(0)}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    return date.toLocaleString();
  };

  const getRouteColor = (route: string) => {
    const colors: Record<string, string> = {
      rag_agent: 'bg-blue-100 text-blue-800',
      diagnostic_agent: 'bg-yellow-100 text-yellow-800',
      pricing_agent: 'bg-green-100 text-green-800',
      tutorial_agent: 'bg-purple-100 text-purple-800',
      summarizer: 'bg-gray-100 text-gray-800',
      unknown: 'bg-gray-100 text-gray-800',
    };
    return colors[route] || colors.unknown;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold">Telemetry Dashboard</h2>
          <p className="text-muted-foreground">Monitor request traces, tool calls, and usage statistics</p>
        </div>
        <Button 
          onClick={() => {
            loadTraces(page);
            loadStats();
          }}
          variant="outline"
        >
          <RefreshCw className="w-4 h-4 mr-2" />
          Refresh
        </Button>
      </div>

      <Tabs defaultValue="overview" className="w-full">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="traces">Request Traces</TabsTrigger>
        </TabsList>

        {/* Overview Tab */}
        <TabsContent value="overview" className="space-y-4">
          {stats && (
            <>
              {/* Stats Cards */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Total Requests</CardTitle>
                    <Activity className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">{stats.total_requests}</div>
                    <p className="text-xs text-muted-foreground">Last 7 days</p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Total Tokens</CardTitle>
                    <Cpu className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">{stats.total_tokens.toLocaleString()}</div>
                    <p className="text-xs text-muted-foreground">
                      ~{(stats.total_tokens / stats.total_requests).toFixed(0)} per request
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Total Cost</CardTitle>
                    <DollarSign className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">{formatCost(stats.total_cost)}</div>
                    <p className="text-xs text-muted-foreground">
                      {formatCost(stats.total_cost / stats.total_requests)} per request
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Avg Latency</CardTitle>
                    <Clock className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">{formatDuration(stats.avg_latency_ms)}</div>
                    <p className="text-xs text-muted-foreground">Per request</p>
                  </CardContent>
                </Card>
              </div>

              {/* Requests by Route */}
              <Card>
                <CardHeader>
                  <CardTitle>Requests by Route</CardTitle>
                  <CardDescription>Distribution of queries across specialist agents</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {Object.entries(stats.requests_by_route).map(([route, count]) => {
                      const percentage = ((count as number / stats.total_requests) * 100).toFixed(1);
                      return (
                        <div key={route} className="flex items-center justify-between">
                          <div className="flex items-center space-x-2">
                            <Badge className={getRouteColor(route)}>
                              {route.replace('_', ' ')}
                            </Badge>
                            <span className="text-sm text-muted-foreground">{count} requests</span>
                          </div>
                          <span className="text-sm font-medium">{percentage}%</span>
                        </div>
                      );
                    })}
                  </div>
                </CardContent>
              </Card>
            </>
          )}
        </TabsContent>

        {/* Traces Tab */}
        <TabsContent value="traces" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Recent Traces</CardTitle>
              <CardDescription>Click on a trace to see detailed tool calls and LLM generations</CardDescription>
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="flex justify-center py-8">
                  <RefreshCw className="w-6 h-6 animate-spin text-gray-400" />
                </div>
              ) : (
                <div className="space-y-2">
                  {traces.length === 0 ? (
                    <p className="text-center text-gray-500 py-8">No traces found</p>
                  ) : (
                    traces.map((trace) => (
                      <div key={trace.id} className="border rounded-lg">
                        <button
                          onClick={() => loadTraceDetails(trace.id)}
                          className="w-full p-4 text-left hover:bg-white/5 transition-colors"
                        >
                          <div className="flex items-start justify-between">
                            <div className="flex-1 space-y-2">
                              <div className="flex items-center space-x-2">
                                {expandedTraceId === trace.id ? (
                                  <ChevronDown className="w-4 h-4 text-gray-400" />
                                ) : (
                                  <ChevronRight className="w-4 h-4 text-gray-400" />
                                )}
                                <Badge className={getRouteColor(trace.route)}>
                                  {trace.route.replace('_', ' ')}
                                </Badge>
                                <Badge variant="outline">{trace.intent}</Badge>
                                <span className="text-sm text-gray-500">
                                  {formatTimestamp(trace.timestamp)}
                                </span>
                              </div>
                              <div className="ml-6 text-sm">
                                <p className="font-medium">
                                  {typeof trace.input === 'string' 
                                    ? trace.input 
                                    : trace.input?.query || 'No query'}
                                </p>
                              </div>
                              <div className="ml-6 flex items-center space-x-4 text-xs text-muted-foreground">
                                <span className="flex items-center">
                                  <Clock className="w-3 h-3 mr-1" />
                                  {formatDuration(trace.duration_ms)}
                                </span>
                                <span className="flex items-center">
                                  <Cpu className="w-3 h-3 mr-1" />
                                  {trace.total_tokens} tokens
                                </span>
                                <span className="flex items-center">
                                  <DollarSign className="w-3 h-3 mr-1" />
                                  {formatCost(trace.total_cost)}
                                </span>
                              </div>
                            </div>
                          </div>
                        </button>

                        {/* Expanded Details */}
                        {expandedTraceId === trace.id && selectedTrace && (
                          <div className="border-t border-white/10 bg-black/20 p-4 space-y-4">
                            {/* Tool Calls */}
                            {selectedTrace.tool_calls.length > 0 && (
                              <div>
                                <h4 className="font-semibold flex items-center mb-2">
                                  <Terminal className="w-4 h-4 mr-2" />
                                  Tool Calls ({selectedTrace.tool_calls.length})
                                </h4>
                                <div className="space-y-2">
                                  {selectedTrace.tool_calls.map((tool) => (
                                    <div key={tool.id} className="bg-white/5 border border-white/10 p-3 rounded text-sm">
                                      <div className="flex items-center justify-between mb-2">
                                        <span className="font-medium">{tool.name}</span>
                                        <Badge variant={tool.status === 'success' ? 'default' : 'destructive'}>
                                          {tool.status || 'completed'}
                                        </Badge>
                                      </div>
                                      <div className="text-xs text-muted-foreground space-y-1">
                                        <p>Duration: {formatDuration(tool.duration_ms)}</p>
                                        {tool.metadata && Object.keys(tool.metadata).length > 0 && (
                                          <div className="mt-2">
                                            <p className="font-medium">Metadata:</p>
                                            <pre className="bg-black/30 p-2 rounded mt-1 overflow-x-auto">
                                              {JSON.stringify(tool.metadata, null, 2)}
                                            </pre>
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}

                            {/* LLM Generations */}
                            {selectedTrace.llm_generations.length > 0 && (
                              <div>
                                <h4 className="font-semibold flex items-center mb-2">
                                  <Zap className="w-4 h-4 mr-2" />
                                  LLM Generations ({selectedTrace.llm_generations.length})
                                </h4>
                                <div className="space-y-2">
                                  {selectedTrace.llm_generations.map((gen) => (
                                    <div key={gen.id} className="bg-white/5 border border-white/10 p-3 rounded text-sm">
                                      <div className="flex items-center justify-between mb-2">
                                        <span className="font-medium">{gen.name}</span>
                                        <Badge>{gen.model}</Badge>
                                      </div>
                                      <div className="text-xs text-muted-foreground space-y-1">
                                        <p>Duration: {formatDuration(gen.duration_ms)}</p>
                                        <p>Tokens: {gen.tokens}</p>
                                        <p>Cost: {formatCost(gen.cost)}</p>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}

                            {/* Response Output */}
                            <div>
                              <h4 className="font-semibold flex items-center mb-2">
                                <Database className="w-4 h-4 mr-2" />
                                Response
                              </h4>
                              <div className="bg-white/5 border border-white/10 p-3 rounded text-sm">
                                <p className="text-foreground/80 whitespace-pre-wrap break-words">
                                  {typeof selectedTrace.trace.output === 'string'
                                    ? selectedTrace.trace.output
                                    : selectedTrace.trace.output?.response || 'No output'}
                                </p>
                              </div>
                            </div>
                          </div>
                        )}
                      </div>
                    ))
                  )}
                </div>
              )}

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex justify-center items-center space-x-2 mt-4 pt-4 border-t">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => loadTraces(page - 1)}
                    disabled={page === 1 || loading}
                  >
                    Previous
                  </Button>
                  <span className="text-sm text-muted-foreground">
                    Page {page} of {totalPages}
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => loadTraces(page + 1)}
                    disabled={page === totalPages || loading}
                  >
                    Next
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
};

export default TelemetryDashboard;
