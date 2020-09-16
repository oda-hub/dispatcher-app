from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere"

import  numpy  as np

from astropy import wcs

from bokeh.models import FuncTickFormatter, FixedTicker

from bokeh.layouts import row, widgetbox,gridplot
from bokeh.models import CustomJS, Slider,HoverTool,ColorBar,LinearColorMapper,LabelSet,ColumnDataSource
from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.palettes import Plasma256


class Image(object):

    def __init__(self,data,header):
        self.data=data
        self.header=header

    def change_image_contrast(self, attr, old, new):
        # print attr,old,new
        self.fig_im.glyph.color_mapper.update(low=self.graph_min_slider.value, high=self.graph_max_slider.value)


    def get_html_draw(self,w=None,h=None, catalog=None, plot=False, vmin=None, vmax=None):

        #import plotly
        #import plotly.graph_objs as go
        #from plotly.graph_objs import  Layout

        # print('vmin,vmax',vmin,vmax)





        msk = ~np.isnan(self.data)

        if vmin is None:
            min_s = self.data[msk].min()

        if vmax is None:
            max_s = self.data[msk].max()



        r = self.data.shape[0] * 2
        c = self.data.shape[1] * 2



        fig = figure(plot_width=w, plot_height=h, x_range=(0, c * 0.5), y_range=(0, r * 0.5),
                     tools=['pan,box_zoom,box_select,wheel_zoom,reset,save,crosshair'])

        w = wcs.WCS(self.header)
        color_mapper = LinearColorMapper(low=vmin, high=vmax, palette=Plasma256)

        fig_im = fig.image(image=[self.data], x=[0], y=[0], dw=[c * 0.5], dh=[r * 0.5],
                           color_mapper=color_mapper)

        hover = HoverTool(tooltips=[("x", "$x"), ("y", "$y"), ("value", "@image")],
                          renderers=[fig_im])

        fig.add_tools(hover)

        #fig, (ax) = plt.subplots(1, 1, figsize=(4, 3), subplot_kw={'projection': WCS(self.header)})
        #im = ax.imshow(self.data,
        #               origin='lower',
        #               zorder=1,
        #               interpolation='none',
        #               aspect='equal',
        #               cmap=plt.get_cmap('jet'),
        #               vmin=vmin,
        #               vmax=vmax)

        if catalog is not None:

            lon = catalog.ra
            lat = catalog.dec


            if len(lat) > 0.:
                pixcrd = w.wcs_world2pix(np.column_stack((lon, lat)), 0)

                msk = ~np.isnan(pixcrd[:, 0])
                #ax.plot(pixcrd[:, 0][msk], pixcrd[:, 1][msk], 'o', mfc='none')
                source = ColumnDataSource(data=dict(lon=pixcrd[:, 0][msk]+0.5,
                                                    lat=pixcrd[:, 1][msk]+0.5,
                                                    names=catalog.name[msk]))
                #for ID, (x, y) in enumerate(pixcrd):
                #    if msk[ID]:
                #        # print ('xy',(pixcrd[:, 0][ID], pixcrd[:, 1][ID]))
                #        ax.annotate('%s' % catalog.name[ID], xy=(x, y), color='white')
                #print(pixcrd[:][msk])
                fig.scatter(x='lon', y='lat', marker='circle', size=15,
                            line_color="white", fill_color=None, alpha=1.0, source=source)

                labels = LabelSet(x='lon', y='lat', text='names', level='glyph',
                                  x_offset=5, y_offset=5, render_mode='canvas', source=source, text_color='white')

                fig.add_layout(labels)
                #print'cat', catalog[msk]


        color_bar = ColorBar(color_mapper=color_mapper,
                             label_standoff=12, border_line_color=None, location=(0, 0))

        JS_code_slider = """
                   var vmin = low_slider.value;
                   var vmax = high_slider.value;
                   fig_im.glyph.color_mapper.high = vmax;
                   fig_im.glyph.color_mapper.low = vmin;
               """


        callback = CustomJS(args=dict(fig_im=fig_im), code=JS_code_slider)

        self.graph_min_slider = Slider(title="Sig. Min", start=min_s, end=max_s, step=1, value=min_s, callback=callback)
        self.graph_max_slider = Slider(title="Sig. Max", start=min_s, end=max_s, step=1, value=max_s * 0.8,callback=callback)


        self.graph_min_slider.on_change('value', self.change_image_contrast)
        self.graph_max_slider.on_change('value', self.change_image_contrast)

        callback.args["low_slider"] = self.graph_min_slider
        callback.args["high_slider"] = self.graph_max_slider

        #ax.set_xlabel('RA')
        #ax.set_ylabel('DEC')
        #ax.grid(True, color='white')
        #fig.colorbar(im, ax=ax)

        #plugins.connect(fig, plugins.MousePosition(fontsize=14))
        #if plot == True:
        #    print('plot', plot)
        #    mpld3.show()


        fig.add_layout(color_bar, 'right')

        layout = row(
            fig, widgetbox(self.graph_min_slider, self.graph_max_slider),
        )

        #curdoc().add_root(layout)

        #output_file("slider.html", title="slider.py example")
        #from bokeh.io import  show
        #show(layout)

        script, div = components(layout)

        html_dict = {}
        html_dict['script'] = script
        html_dict['div'] = div
        return html_dict

    def get_js9_html(self, file_path,region_file=None,js9_id='myJS9',ext_id='all'):
        region=''
        file='''JS9.Preload("product/%s", {scale: 'linear', colormap: 'plasma'}, {display: "%s"}, {extid: "%s"});'''%(file_path,js9_id,ext_id)
        if region_file is not None:
            region='''JS9.LoadRegions("product/%s", {display: "%s"});\n'''%(region_file,js9_id)
        t = '''                                                                                                                                                                             
    <html>                                                                                                                                                                                     
                <head>                                                                                                                                                                         
                  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">                                                                                                          
                  <meta http-equiv="X-UA-Compatible" content="IE=Edge;chrome=1" >                                                                                                              
                  <meta name="viewport" content="width=device-width, initial-scale=1">                                                                                                         
                  <link type="image/x-icon" rel="shortcut icon" href="./favicon.ico">                                                                                                          
                  <link type="text/css" rel="stylesheet" href="js9/js9support.css">                                                                                                            
                  <link type="text/css" rel="stylesheet" href="js9/js9.css">                                                                                                                   
                  <script type="text/javascript" src="js9/js9prefs.js"></script>                                                                                                               
                  <script type="text/javascript" src="js9/js9support.min.js"></script>                                                                                                         
                  <script type="text/javascript" src="js9/js9.min.js"></script>                                                                                                                
                  <script type="text/javascript" src="js9/js9plugins.js"></script>                                                                                                             
                    </head>                                                                                                                                                                    
                <body>                                                                                                                                                                         


                <center><font size="+1">                                                                                                                                                       
                </font></center>                                                                                                                                                               
                <table cellspacing="30">                                                                                                                                                       
                <tr valign="top">                                                                                                                                                              
        <td>                                                                                                                                                                                   
                </td>                                                                                                                                                                          
        <td>                                                                                                                                                                                   
                <tr valign="top">                                                                                                                                                              
                <td>                                                                                                                                                                           
                <div class="JS9Menubar"  id="%sMenubar" ></div>
                <div class="JS9Colorbar" id="%sColorbar" ></div>
                <div class="JS9" id="%s"></div>                                                                                                                                                        
                </td>                                                                                                                                                                          
                <td>                                                                                                                                                                           

                <p>                                                                                                                                                                            
                </td>                                                                                                                                                                          
                </tr>                                                                                                                                                                          
                </table>                                                                                                                                                                       
                <script type="text/javascript">                                                                                                                                                
                  function init(){                                                                                                                                                             
                     var idx, obj;                                                                                                                                                             
                     %s
                     %s                                                                                                                      
                  }                                                                                                                                                                            
                  $(document).ready(function(){                                                                                                                                                
                    init();                                                                                                                                                                    
                  });                                                                                                                                                                          
                </script>                                                                                                                                                                      

            </body>                                                                                                                                                                            
    </html>                                                                                                                                                                                    

    ''' % (js9_id,js9_id,js9_id,file,region)

        return t

class ScatterPlot(object):


    def __init__(self,w,h,x_label=None,y_label=None,x_range=None,y_range=None,title=None,y_axis_type='linear',x_axis_type='linear'):
        hover = HoverTool(tooltips=[("x", "$x"), ("y", "$y")])

        self.fig = figure(title=title, width=w, height=h,x_range=x_range,y_range=y_range,
                          y_axis_type=y_axis_type,
                          x_axis_type=x_axis_type,
                     tools=[hover, 'pan,box_zoom,box_select,wheel_zoom,reset,save,crosshair']
                     )

        if x_label is not None:
            self.fig.xaxis.axis_label = x_label

        if y_label is not None:
            self.fig.yaxis.axis_label = y_label

    def add_errorbar(self, x, y, xerr=None, yerr=None, color='red',
                 point_kwargs={}, error_kwargs={}):

        self.fig.circle(x, y, color=color, **point_kwargs)

        if xerr is not None:
            x_err_x = []
            x_err_y = []
            for px, py, err in zip(x, y, xerr):
                x_err_x.append((px - err, px + err))
                x_err_y.append((py, py))
            self.fig.multi_line(x_err_x, x_err_y, color=color, **error_kwargs)

        if yerr is not None:
            y_err_x = []
            y_err_y = []
            for px, py, err in zip(x, y, yerr):
                y_err_x.append((px, px))
                y_err_y.append((py - err, py + err))
            self.fig.multi_line(y_err_x, y_err_y, color=color, **error_kwargs)



    def add_step_line(self,x,y,legend=None):
        #print('a')
        self.fig.step(x,y,name=legend, mode="center")
        #print('b')

    def add_line(self,x,y,legend=None,color=None):
        self.fig.line(x,y,legend=legend,line_color=color)

    def get_html_draw(self):



        layout = row(
            self.fig
        )
        #curdoc().add_root(layout)


        #show(layout)

        script, div = components(layout)

        #print ('script',script)
        #print ('div',div)

        html_dict = {}
        html_dict['script'] = script
        html_dict['div'] = div
        return html_dict


class GridPlot(object):

    def __init__(self,f1,f2,w=None,h=None):

        self.f1=f1
        self.f2=f2

    def get_html_draw(self,w=None,h=None):
        #l = layout([self.f1.fig],[self.f2.fig])


        grid = gridplot([self.f1.fig,self.f2.fig],ncols=1,plot_width=w, plot_height=h)
        #curdoc().add_root(grid)
        #show(grid)
        #output_file("test.html")
        script, div = components(grid)

        html_dict={}
        html_dict['script']=script
        html_dict['div'] = div
        return html_dict
